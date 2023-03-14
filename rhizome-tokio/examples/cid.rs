use anyhow::Result;
use cid::Cid;
use futures::{sink::unfold, StreamExt};
use rhizome::{
    fact::{evac_fact::EVACFact, traits::EDBFact},
    logic::lower_to_ram,
    runtime::client::Client,
    types::Any,
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let program = rhizome::build(|p| {
        p.input("evac", |h| {
            h.column::<Cid>("cid")
                .column::<Any>("entity")
                .column::<Any>("attribute")
                .column::<Any>("value")
        })?;

        p.output("create", |h| {
            h.column::<Cid>("cid")
                .column::<i32>("entity")
                .column::<i32>("initial")
        })?;

        p.output("update", |h| {
            h.column::<Cid>("cid")
                .column::<i32>("entity")
                .column::<Cid>("parent")
                .column::<i32>("value")
        })?;

        p.output("head", |h| {
            h.column::<Cid>("cid")
                .column::<i32>("entity")
                .column::<i32>("value")
        })?;

        p.rule::<(Cid, i32, i32)>("create", &|h, b, (cid, e, i)| {
            (
                h.bind((("cid", cid), ("entity", e), ("initial", i))),
                b.search(
                    "evac",
                    (
                        ("cid", cid),
                        ("entity", e),
                        ("attribute", "initial"),
                        ("value", i),
                    ),
                ),
            )
        })?;

        p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
            (
                h.bind((
                    ("cid", cid),
                    ("entity", e),
                    ("value", v),
                    ("parent", parent),
                )),
                b.search(
                    "evac",
                    (
                        ("cid", cid),
                        ("entity", e),
                        ("attribute", "write"),
                        ("value", v),
                    ),
                )
                .get_link(cid, "parent", parent)
                .search("create", (("cid", parent), ("entity", e))),
            )
        })?;

        p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
            (
                h.bind((
                    ("cid", cid),
                    ("entity", e),
                    ("value", v),
                    ("parent", parent),
                )),
                b.search(
                    "evac",
                    (
                        ("cid", cid),
                        ("entity", e),
                        ("attribute", "write"),
                        ("value", v),
                    ),
                )
                .get_link(cid, "parent", parent)
                .search("update", (("cid", parent), ("entity", e))),
            )
        })?;

        p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
            (
                h.bind((("cid", cid), ("entity", e), ("value", v))),
                b.search("create", (("cid", cid), ("entity", e), ("initial", v)))
                    .except("update", (("entity", e), ("parent", cid))),
            )
        })?;

        p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
            (
                h.bind((("cid", cid), ("entity", e), ("value", v))),
                b.search("update", (("cid", cid), ("entity", e), ("value", v)))
                    .except("update", (("entity", e), ("parent", cid))),
            )
        })
    })?;

    let program = lower_to_ram::lower_to_ram(&program)?;

    let (mut client, mut rx, reactor) = Client::new(program);

    spawn(async move { reactor.async_run().await });
    spawn(async move {
        loop {
            let _ = rx.next().await;
        }
    });

    let e0 = EVACFact::new(0, "initial", 0, vec![]);
    let e1 = EVACFact::new(0, "write", 1, vec![("parent", e0.cid())]);
    let e2 = EVACFact::new(0, "write", 5, vec![("parent", e1.cid())]);
    let e3 = EVACFact::new(0, "write", 3, vec![("parent", e1.cid())]);
    let e4 = EVACFact::new(1, "initial", 4, vec![]);

    client
        .register_sink(
            "create",
            Box::new(|| {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

                    Ok(())
                }))
            }),
        )
        .await?;

    client
        .register_sink(
            "update",
            Box::new(|| {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

                    Ok(())
                }))
            }),
        )
        .await?;

    client
        .register_sink(
            "head",
            Box::new(|| {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

                    Ok(())
                }))
            }),
        )
        .await?;

    client.insert_fact(e0).await?;
    client.insert_fact(e1).await?;
    client.insert_fact(e2).await?;
    client.insert_fact(e3).await?;
    client.insert_fact(e4).await?;
    client.flush().await?;

    Ok(())
}

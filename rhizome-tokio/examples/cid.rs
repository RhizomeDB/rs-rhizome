use anyhow::Result;
use cid::Cid;
use futures::{sink::unfold, StreamExt};
use rhizome::{
    builder::ProgramBuilder,
    fact::{evac_fact::EVACFact, traits::EDBFact},
    logic::lower_to_ram,
    runtime::client::Client,
    storage::content_addressable::ContentAddressable,
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let program = ProgramBuilder::build(|p| {
        p.input("evac", |h| {
            h.column::<Cid>("cid")
                .column::<i32>("entity")
                .column::<&str>("attribute")
                .column::<i32>("value")
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
                h.bind("cid", cid).bind("entity", e).bind("initial", i),
                b.search("evac", |s| {
                    s.bind("cid", cid)
                        .bind("entity", e)
                        .bind("value", i)
                        .when("attribute", "initial")
                }),
            )
        })?;

        p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
            (
                h.bind("cid", cid)
                    .bind("entity", e)
                    .bind("value", v)
                    .bind("parent", parent),
                b.search("evac", |s| {
                    s.bind("cid", cid)
                        .bind("entity", e)
                        .bind("value", v)
                        .when("attribute", "write")
                })
                .get_link(cid, "parent", parent)
                .search("create", |s| s.bind("cid", parent).bind("entity", e)),
            )
        })?;

        p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
            (
                h.bind("cid", cid)
                    .bind("entity", e)
                    .bind("value", v)
                    .bind("parent", parent),
                b.search("evac", |s| {
                    s.bind("cid", cid)
                        .bind("entity", e)
                        .bind("value", v)
                        .when("attribute", "write")
                })
                .get_link(cid, "parent", parent)
                .search("update", |s| s.bind("cid", parent).bind("entity", e)),
            )
        })?;

        p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
            (
                h.bind("cid", cid).bind("entity", e).bind("value", v),
                b.search("create", |s| {
                    s.bind("cid", cid).bind("entity", e).bind("initial", v)
                })
                .except("update", |s| s.bind("entity", e).bind("parent", cid)),
            )
        })?;

        p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
            (
                h.bind("cid", cid).bind("entity", e).bind("value", v),
                b.search("update", |s| {
                    s.bind("cid", cid).bind("entity", e).bind("value", v)
                })
                .except("update", |s| s.bind("entity", e).bind("parent", cid)),
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

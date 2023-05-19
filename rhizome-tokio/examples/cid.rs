use anyhow::Result;
use cid::Cid;
use futures::{sink::unfold, StreamExt};
use rhizome::{
    fact::{
        evac_fact::EVACFact,
        traits::{EDBFact, Fact},
    },
    runtime::client::Client,
    types::Any,
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (mut client, mut rx, reactor) = Client::new();

    spawn(async move {
        reactor
            .async_run(|p| {
                p.input("evac", |h| {
                    h.column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("create", |h| {
                    h.column::<i32>("entity").column::<i32>("initial")
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
                    h.bind((("cid", cid), ("entity", e), ("initial", i)))?;
                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "initial"), ("value", i)),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
                    h.bind((
                        ("cid", cid),
                        ("entity", e),
                        ("value", v),
                        ("parent", parent),
                    ))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "write"), ("value", v)),
                    )?;
                    b.get_link(cid, "parent", parent)?;
                    b.search("create", (("cid", parent), ("entity", e)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
                    h.bind((
                        ("cid", cid),
                        ("entity", e),
                        ("value", v),
                        ("parent", parent),
                    ))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "write"), ("value", v)),
                    )?;
                    b.get_link(cid, "parent", parent)?;
                    b.search("update", (("cid", parent), ("entity", e)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
                    h.bind((("cid", cid), ("entity", e), ("value", v)))?;
                    b.search("create", (("cid", cid), ("entity", e), ("initial", v)))?;
                    b.except("update", (("entity", e), ("parent", cid)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
                    h.bind((("cid", cid), ("entity", e), ("value", v)))?;

                    b.search("update", (("cid", cid), ("entity", e), ("value", v)))?;
                    b.except("update", (("entity", e), ("parent", cid)))?;

                    Ok(())
                })?;

                Ok(p)
            })
            .await
    });

    spawn(async move {
        loop {
            let _ = rx.next().await;
        }
    });

    let e0 = EVACFact::new(0, "initial", 0, vec![]);
    let e1 = EVACFact::new(0, "write", 1, vec![("parent".into(), e0.cid()?.unwrap())]);
    let e2 = EVACFact::new(0, "write", 5, vec![("parent".into(), e1.cid()?.unwrap())]);
    let e3 = EVACFact::new(0, "write", 3, vec![("parent".into(), e1.cid()?.unwrap())]);
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

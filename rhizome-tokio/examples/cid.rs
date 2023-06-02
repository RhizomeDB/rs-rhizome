use anyhow::Result;
use cid::Cid;
use futures::{sink::unfold, StreamExt};
use rhizome::{
    fact::{
        evac_fact::EVACFact,
        traits::{EDBFact, Fact},
    },
    kernel::math,
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
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("value")
                })?;

                p.output("update", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<Cid>("parent")
                        .column::<i32>("value")
                })?;

                p.output("lowestChild", |h| {
                    h.column::<Cid>("child").column::<Cid>("parent")
                })?;

                p.output("selectedWrite", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("value")
                })?;

                p.output("root", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("value")
                })?;

                p.output("head", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("value")
                })?;

                p.rule::<(Cid, i32, i32)>("create", &|h, b, (cid, e, i)| {
                    h.bind((("cid", cid), ("key", e), ("value", i)))?;
                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "initial"), ("value", i)),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
                    h.bind((("cid", cid), ("key", e), ("value", v), ("parent", parent)))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "write"), ("value", v)),
                    )?;
                    b.get_link(cid, "parent", parent)?;
                    b.search("create", (("cid", parent), ("key", e)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, e, v, parent)| {
                    h.bind((("cid", cid), ("key", e), ("value", v), ("parent", parent)))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", e), ("attribute", "write"), ("value", v)),
                    )?;
                    b.get_link(cid, "parent", parent)?;
                    b.search("update", (("cid", parent), ("key", e)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid)>("lowestChild", &|h, b, (child, parent)| {
                    h.bind((("child", child), ("parent", parent)))?;

                    b.search("update", (("parent", parent),))?;
                    b.group_by(
                        child,
                        "update",
                        (("parent", parent), ("cid", child)),
                        math::min(child),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("selectedWrite", &|h, b, (cid, e, v)| {
                    h.bind((("cid", cid), ("key", e), ("value", v)))?;
                    b.search("root", (("cid", cid), ("key", e), ("value", v)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid, i32, i32)>("selectedWrite", &|h, b, (cid, parent, e, v)| {
                    h.bind((("cid", cid), ("key", e), ("value", v)))?;

                    b.search("selectedWrite", (("cid", parent), ("key", e)))?;
                    b.search("lowestChild", (("parent", parent), ("child", cid)))?;
                    b.search("update", (("cid", cid), ("value", v)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("root", &|h, b, (cid, e, v)| {
                    h.bind((("cid", cid), ("key", e), ("value", v)))?;

                    b.search("create", (("key", e), ("value", v)))?;
                    b.group_by(cid, "create", (("key", e), ("cid", cid)), math::min(cid))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, e, v)| {
                    h.bind((("cid", cid), ("key", e), ("value", v)))?;

                    b.search("selectedWrite", (("cid", cid), ("key", e), ("value", v)))?;
                    b.except("update", (("key", e), ("parent", cid)))?;

                    Ok(())
                })?;

                Ok(p)
            })
            .await
            .unwrap()
    });

    spawn(async move {
        loop {
            let _ = rx.next().await;
        }
    });

    client
        .register_sink(
            "head",
            Box::new(|| {
                Box::new(unfold((), move |(), fact| async move {
                    println!("{fact}");

                    Ok(())
                }))
            }),
        )
        .await?;

    let e0 = EVACFact::new(0, "initial", 0, vec![]);
    let e1 = EVACFact::new(0, "write", 1, vec![("parent".into(), e0.cid()?.unwrap())]);
    let e2 = EVACFact::new(0, "write", 5, vec![("parent".into(), e1.cid()?.unwrap())]);
    let e3 = EVACFact::new(0, "write", 3, vec![("parent".into(), e1.cid()?.unwrap())]);
    let e4 = EVACFact::new(1, "initial", 4, vec![]);
    let e5 = EVACFact::new(0, "write", 6, vec![("parent".into(), e0.cid()?.unwrap())]);
    let e6 = EVACFact::new(0, "write", 7, vec![("parent".into(), e0.cid()?.unwrap())]);

    assert!(e2.cid()?.unwrap() < e3.cid()?.unwrap());
    assert!(e5.cid()?.unwrap() < e1.cid()?.unwrap());
    assert!(e5.cid()?.unwrap() < e6.cid()?.unwrap());

    client.insert_fact(e0).await?;
    client.insert_fact(e1).await?;
    client.insert_fact(e2).await?;
    client.insert_fact(e3).await?;
    client.insert_fact(e4).await?;
    client.insert_fact(e5).await?;
    client.insert_fact(e6).await?;

    client.flush().await?;

    Ok(())
}

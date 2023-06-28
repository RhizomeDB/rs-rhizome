use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use cid::Cid;
use futures::{sink::unfold, StreamExt};
use rhizome::{
    kernel::math,
    runtime::client::Client,
    tuple::{InputTuple, Tuple},
    value::Val,
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (mut client, mut rx, reactor) = Client::new();

    spawn(async move {
        reactor
            .async_run(|p| {
                p.output("create", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("val")
                })?;

                p.output("update", |h| {
                    h.column::<Cid>("cid")
                        .column::<Cid>("parent")
                        .column::<i32>("key")
                        .column::<i32>("val")
                })?;

                p.output("selectedWrite", |h| {
                    h.column::<Cid>("cid").column::<i32>("key")
                })?;

                p.output("head", |h| {
                    h.column::<Cid>("cid")
                        .column::<i32>("key")
                        .column::<i32>("val")
                })?;

                p.rule::<(Cid, i32, i32)>("create", &|h, b, (cid, k, v)| {
                    h.bind((("cid", cid), ("key", k), ("val", v)))?;
                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", k), ("attribute", "create"), ("value", v)),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, k, v, parent)| {
                    h.bind((("cid", cid), ("key", k), ("val", v), ("parent", parent)))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", k), ("attribute", "update"), ("value", v)),
                    )?;
                    b.search("links", (("from", cid), ("to", parent)))?;
                    b.search("create", (("cid", parent), ("key", k)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32, Cid)>("update", &|h, b, (cid, k, v, parent)| {
                    h.bind((("cid", cid), ("key", k), ("val", v), ("parent", parent)))?;

                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", k), ("attribute", "update"), ("value", v)),
                    )?;
                    b.search("links", (("from", cid), ("to", parent)))?;
                    b.search("update", (("cid", parent), ("key", k)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32)>("selectedWrite", &|h, b, (cid, k)| {
                    h.bind((("cid", cid), ("key", k)))?;

                    b.search("create", (("key", k),))?;
                    b.group_by(cid, "create", (("key", k), ("cid", cid)), math::min(cid))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid, i32)>("selectedWrite", &|h, b, (cid, parent, k)| {
                    h.bind((("cid", cid), ("key", k)))?;

                    b.search("selectedWrite", (("cid", parent), ("key", k)))?;
                    b.group_by(
                        cid,
                        "update",
                        (("parent", parent), ("cid", cid)),
                        math::min(cid),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, k, v)| {
                    h.bind((("cid", cid), ("key", k), ("val", v)))?;

                    b.search("selectedWrite", (("cid", cid), ("key", k)))?;
                    b.search("create", (("cid", cid), ("val", v)))?;
                    b.except("update", (("key", k), ("parent", cid)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, i32, i32)>("head", &|h, b, (cid, k, v)| {
                    h.bind((("cid", cid), ("key", k), ("val", v)))?;

                    b.search("selectedWrite", (("cid", cid), ("key", k)))?;
                    b.search("update", (("cid", cid), ("val", v)))?;
                    b.except("update", (("key", k), ("parent", cid)))?;

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

    let kv: Arc<RwLock<HashMap<Val, Val>>> = Arc::default();

    client
        .register_sink(
            "head",
            Box::new({
                let kv = Arc::clone(&kv);
                || {
                    Box::new(unfold(kv, move |kv, fact: Tuple| async move {
                        let k = fact.col(&"key".into()).unwrap();
                        let v = fact.col(&"val".into()).unwrap();

                        kv.write().unwrap().insert(k, v);

                        Ok(kv)
                    }))
                }
            }),
        )
        .await?;

    let e0 = InputTuple::new(0, "create", 0, vec![]);
    let e1 = InputTuple::new(0, "update", 1, vec![e0.cid()?]);
    let e2 = InputTuple::new(0, "update", 5, vec![e1.cid()?]);
    let e3 = InputTuple::new(0, "update", 3, vec![e1.cid()?]);
    let e4 = InputTuple::new(1, "create", 4, vec![]);
    let e5 = InputTuple::new(0, "update", 14, vec![e0.cid()?]);
    let e6 = InputTuple::new(0, "update", 15, vec![e0.cid()?]);
    let e7 = InputTuple::new(0, "update", 9, vec![e4.cid()?]);
    let e8 = InputTuple::new(0, "create", 12, vec![]);
    let e9 = InputTuple::new(0, "create", 26, vec![]);

    assert!(e2.cid()? < e3.cid()?);
    assert!(e5.cid()? < e1.cid()?);
    assert!(e5.cid()? < e6.cid()?);
    assert!(e0.cid()? < e8.cid()?);
    assert!(e0.cid()? > e9.cid()?);

    client.insert_fact(e0).await?;
    client.insert_fact(e1).await?;
    client.insert_fact(e2).await?;
    client.insert_fact(e3).await?;
    client.insert_fact(e4).await?;
    client.insert_fact(e5).await?;
    client.insert_fact(e6).await?;
    client.insert_fact(e7).await?;
    client.flush().await?;

    assert_eq!(kv.read().unwrap().get(&Val::S32(0)), Some(&14.into()));
    assert_eq!(kv.read().unwrap().get(&Val::S32(1)), Some(&4.into()));

    // Adding a new root with a larger CID doesn't change the value
    client.insert_fact(e8).await?;
    client.flush().await?;

    assert_eq!(kv.read().unwrap().get(&Val::S32(0)), Some(&14.into()));

    // Adding a new root with a smaller CID changes the value
    client.insert_fact(e9).await?;
    client.flush().await?;

    assert_eq!(kv.read().unwrap().get(&Val::S32(0)), Some(&26.into()));

    println!("{:?}", kv.read().unwrap());

    Ok(())
}

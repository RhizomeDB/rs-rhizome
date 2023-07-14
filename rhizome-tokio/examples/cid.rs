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
    value::{Any, Val},
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (mut client, mut rx, reactor) = Client::new();

    spawn(async move {
        reactor
            .async_run(|p| {
                p.output("set", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("store")
                        .column::<Any>("key")
                        .column::<Any>("val")
                })?;

                p.output("root", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("store")
                        .column::<Any>("key")
                })?;

                p.output("child", |h| h.column::<Cid>("cid").column::<Cid>("parent"))?;
                p.output("latestSibling", |h| h.column::<Cid>("cid"))?;

                p.output("head", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("store")
                        .column::<Any>("key")
                        .column::<Any>("val")
                })?;

                p.rule::<(Cid, Any, Any, Any)>("set", &|h, b, (cid, store, k, v)| {
                    h.bind((("cid", cid), ("store", store), ("key", k), ("val", v)))?;
                    b.search_cid(
                        "evac",
                        cid,
                        (("entity", store), ("attribute", k), ("value", v)),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, Any, Any)>("root", &|h, b, (cid, store, k)| {
                    h.bind((("cid", cid), ("store", store), ("key", k)))?;

                    b.search("set", (("cid", cid), ("store", store), ("key", k)))?;
                    b.except("links", (("from", cid),))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid, Any, Any)>("child", &|h, b, (cid, parent, store, k)| {
                    h.bind((("cid", cid), ("parent", parent)))?;

                    b.search("set", (("cid", cid), ("store", store), ("key", k)))?;
                    b.search("links", (("from", cid), ("to", parent)))?;
                    b.search("root", (("cid", parent), ("store", store), ("key", k)))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid, Any, Any)>("child", &|h, b, (cid, parent, store, k)| {
                    h.bind((("cid", cid), ("parent", parent)))?;

                    b.search("set", (("cid", cid), ("store", store), ("key", k)))?;
                    b.search("links", (("from", cid), ("to", parent)))?;
                    b.search("child", (("cid", parent),))?;

                    Ok(())
                })?;

                p.rule::<(Cid, Any, Any)>("latestSibling", &|h, b, (cid, store, k)| {
                    h.bind((("cid", cid),))?;

                    b.search("root", (("store", store), ("key", k)))?;
                    b.group_by(
                        cid,
                        "root",
                        (("cid", cid), ("store", store), ("key", k)),
                        math::min(cid),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, Cid)>("latestSibling", &|h, b, (cid, parent)| {
                    h.bind((("cid", cid),))?;

                    b.search("latestSibling", (("cid", parent),))?;
                    b.group_by(
                        cid,
                        "child",
                        (("cid", cid), ("parent", parent)),
                        math::min(cid),
                    )?;

                    Ok(())
                })?;

                p.rule::<(Cid, Any, Any, Any)>("head", &|h, b, (cid, store, k, v)| {
                    h.bind((("cid", cid), ("store", store), ("key", k), ("val", v)))?;

                    b.search("latestSibling", (("cid", cid),))?;
                    b.search(
                        "set",
                        (("cid", cid), ("store", store), ("key", k), ("val", v)),
                    )?;
                    b.except("child", (("parent", cid),))?;

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

    let kv: Arc<RwLock<HashMap<(Val, Val), Val>>> = Arc::default();

    client
        .register_sink(
            "head",
            Box::new({
                let kv = Arc::clone(&kv);
                || {
                    Box::new(unfold(kv, move |kv, tuple: Tuple| async move {
                        let store = tuple.col(&"store".into()).unwrap();
                        let k = tuple.col(&"key".into()).unwrap();
                        let v = tuple.col(&"val".into()).unwrap();

                        kv.write().unwrap().insert((store, k), v);

                        Ok(kv)
                    }))
                }
            }),
        )
        .await?;

    let e0 = InputTuple::new(0_i32, 0, 0, vec![]);
    let e1 = InputTuple::new(0_i32, 0, 2, vec![e0.cid()?]);
    let e2 = InputTuple::new(0_i32, 0, 4, vec![e1.cid()?]);
    let e3 = InputTuple::new(0_i32, 0, 5, vec![e1.cid()?]);
    let e4 = InputTuple::new(0_i32, 1, 5, vec![]);
    let e5 = InputTuple::new(0_i32, 0, 6, vec![e0.cid()?]);
    let e6 = InputTuple::new(0_i32, 0, 16, vec![e0.cid()?]);
    let e7 = InputTuple::new(0_i32, 0, 9, vec![e4.cid()?]);
    let e8 = InputTuple::new(0_i32, 0, 22, vec![]);
    let e9 = InputTuple::new(0_i32, 0, 24, vec![]);
    let e10 = InputTuple::new(0_i32, 0, 86, vec![e5.cid()?]);
    let e11 = InputTuple::new(1_i32, 0, 2, vec![]);

    assert!(e2.cid()? < e3.cid()?);
    assert!(e5.cid()? < e1.cid()?);
    assert!(e5.cid()? < e6.cid()?);
    assert!(e0.cid()? < e8.cid()?);
    assert!(e0.cid()? > e9.cid()?);
    assert!(e10.cid()? < e9.cid()?);
    assert!(e11.cid()? < e0.cid()?);

    client.insert_tuple(e0).await?;
    client.insert_tuple(e1).await?;
    client.insert_tuple(e2).await?;
    client.insert_tuple(e3).await?;
    client.insert_tuple(e4).await?;
    client.insert_tuple(e5).await?;
    client.insert_tuple(e6).await?;
    client.insert_tuple(e7).await?;
    client.flush().await?;

    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 0.into())),
        Some(&6.into())
    );
    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 1.into())),
        Some(&5.into())
    );

    // Adding a new root with a larger CID doesn't change the value
    client.insert_tuple(e8).await?;
    client.flush().await?;

    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 0.into())),
        Some(&6.into())
    );

    // Adding a new root with a smaller CID changes the value
    client.insert_tuple(e9).await?;
    client.flush().await?;

    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 0.into())),
        Some(&24.into())
    );

    // Adding a new child under a previously latest write doesn't change the value
    client.insert_tuple(e10).await?;
    client.flush().await?;

    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 0.into())),
        Some(&24.into())
    );

    // Writing a more recent value to another KV doesn't change the value in other KVs
    client.insert_tuple(e11).await?;
    client.flush().await?;

    assert_eq!(
        kv.read().unwrap().get(&(0.into(), 0.into())),
        Some(&24.into())
    );

    println!("{:?}", kv.read().unwrap());

    Ok(())
}

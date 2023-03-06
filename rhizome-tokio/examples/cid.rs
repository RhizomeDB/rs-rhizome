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
            h.column::<Cid>("cid")?
                .column::<i32>("entity")?
                .column::<&str>("attribute")?
                .column::<i32>("value")
        })?;

        p.output("create", |h| {
            h.column::<Cid>("cid")?
                .column::<i32>("entity")?
                .column::<i32>("initial")
        })?;

        p.output("update", |h| {
            h.column::<Cid>("cid")?
                .column::<i32>("entity")?
                .column::<Cid>("parent")?
                .column::<i32>("value")
        })?;

        p.output("head", |h| {
            h.column::<Cid>("cid")?
                .column::<i32>("entity")?
                .column::<i32>("value")
        })?;

        p.rule(
            "create",
            |h| {
                h.bind("cid", "Cid")?
                    .bind("entity", "E")?
                    .bind("initial", "I")
            },
            |b| {
                b.search("evac", |s| {
                    s.bind("cid", "Cid")?
                        .bind("entity", "E")?
                        .bind("value", "I")?
                        .when("attribute", "initial")
                })
            },
        )?;

        p.rule(
            "update",
            |h| {
                h.bind("cid", "Cid")?
                    .bind("entity", "E")?
                    .bind("value", "V")?
                    .bind("parent", "P")
            },
            |b| {
                b.search("evac", |s| {
                    s.bind("cid", "Cid")?
                        .bind("entity", "E")?
                        .bind("value", "V")?
                        .when("attribute", "write")
                })?
                .get_link("Cid", "parent", "P")?
                .search("create", |s| s.bind("cid", "P")?.bind("entity", "E"))
            },
        )?;

        p.rule(
            "update",
            |h| {
                h.bind("cid", "Cid")?
                    .bind("entity", "E")?
                    .bind("value", "V")?
                    .bind("parent", "P")
            },
            |b| {
                b.search("evac", |s| {
                    s.bind("cid", "Cid")?
                        .bind("entity", "E")?
                        .bind("value", "V")?
                        .when("attribute", "write")
                })?
                .get_link("Cid", "parent", "P")?
                .search("update", |s| s.bind("cid", "P")?.bind("entity", "E"))
            },
        )?;

        p.rule(
            "head",
            |h| {
                h.bind("cid", "Cid")?
                    .bind("entity", "E")?
                    .bind("value", "V")
            },
            |b| {
                b.search("create", |s| {
                    s.bind("cid", "Cid")?
                        .bind("entity", "E")?
                        .bind("initial", "V")
                })?
                .except("update", |s| s.bind("entity", "E")?.bind("parent", "Cid"))
            },
        )?;

        p.rule(
            "head",
            |h| {
                h.bind("cid", "Cid")?
                    .bind("entity", "E")?
                    .bind("value", "V")
            },
            |b| {
                b.search("update", |s| {
                    s.bind("cid", "Cid")?
                        .bind("entity", "E")?
                        .bind("value", "V")
                })?
                .except("update", |s| s.bind("entity", "E")?.bind("parent", "Cid"))
            },
        )
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

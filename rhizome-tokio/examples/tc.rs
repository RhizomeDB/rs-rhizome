use anyhow::Result;
use futures::{sink::unfold, StreamExt};
use rhizome::{fact::traits::EDBFact, logic::lower_to_ram, runtime::client::Client, types::Any};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let program = rhizome::build(|p| {
        p.input("evac", |h| {
            h.column::<Any>("entity")
                .column::<Any>("attribute")
                .column::<Any>("value")
        })?;

        p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
        p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

        p.rule::<(i32, i32)>("edge", &|h, b, (x, y)| {
            (
                h.bind((("from", x), ("to", y))),
                b.search("evac", (("entity", x), ("attribute", "to"), ("value", y))),
            )
        })?;

        p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
            (
                h.bind((("from", x), ("to", y))),
                b.search("edge", (("from", x), ("to", y))),
            )
        })?;

        p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
            (
                h.bind((("from", x), ("to", z))),
                b.search("edge", (("from", x), ("to", y)))
                    .search("path", (("from", y), ("to", z))),
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

    client
        .register_sink(
            "path",
            Box::new(|| {
                Box::new(unfold((), move |(), fact| async move {
                    println!("{fact}");

                    Ok(())
                }))
            }),
        )
        .await?;

    client.insert_fact(EDBFact::new(0, "to", 1, vec![])).await?;
    client.insert_fact(EDBFact::new(1, "to", 2, vec![])).await?;
    client.insert_fact(EDBFact::new(2, "to", 3, vec![])).await?;
    client.insert_fact(EDBFact::new(3, "to", 4, vec![])).await?;
    client.flush().await?;

    Ok(())
}

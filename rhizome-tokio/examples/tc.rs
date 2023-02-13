use anyhow::Result;
use futures::{channel::oneshot, sink::unfold, stream};
use rhizome::{fact::Fact, logic::parser};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = oneshot::channel();

    spawn(async move {
        let program = parser::parse(
            r#"
    input edge(from, to).

    path(from: X, to: Y) :- edge(from: X, to: Y).
    path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
    "#,
        )
        .unwrap();

        let mut reactor = rhizome::spawn(&program).unwrap();
        reactor
            .register_stream(move || {
                Box::new(stream::iter([
                    Fact::new("edge", [("from", 0), ("to", 1)]),
                    Fact::new("edge", [("from", 1), ("to", 2)]),
                    Fact::new("edge", [("from", 2), ("to", 3)]),
                    Fact::new("edge", [("from", 3), ("to", 4)]),
                ]))
            })
            .unwrap();

        reactor
            .register_sink("path", move || {
                Box::new(unfold((), move |_, fact: Fact| async move {
                    println!("{}", fact);

                    Ok(())
                }))
            })
            .unwrap();

        reactor.subscribe(tx);

        reactor.async_run().await.unwrap();
    });

    rx.await?;

    Ok(())
}

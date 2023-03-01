use anyhow::Result;
use cid::Cid;
use futures::{channel::oneshot, sink::unfold, stream};
use rhizome::{datum::Datum, fact::traits::EDBFact, id::LinkId, logic::parser};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = oneshot::channel();

    spawn(async move {
        let program = parser::parse(
            r#"
            input evac(cid, entity, attribute, value).

            output edge(from, to).
            output path(from, to).

            edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

            path(from: X, to: Y) :- edge(from: X, to: Y).
            path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
            "#,
        )
        .unwrap();

        let mut reactor = rhizome::spawn(&program).unwrap();
        reactor
            .register_stream(move || {
                Box::new(stream::iter([
                    EDBFact::new(
                        "evac",
                        [
                            ("entity", Datum::int(0)),
                            ("attribute", Datum::string("to")),
                            ("value", Datum::int(1)),
                        ],
                        Vec::<(LinkId, Cid)>::default(),
                    ),
                    EDBFact::new(
                        "evac",
                        [
                            ("entity", Datum::int(1)),
                            ("attribute", Datum::string("to")),
                            ("value", Datum::int(2)),
                        ],
                        Vec::<(LinkId, Cid)>::default(),
                    ),
                    EDBFact::new(
                        "evac",
                        [
                            ("entity", Datum::int(2)),
                            ("attribute", Datum::string("to")),
                            ("value", Datum::int(3)),
                        ],
                        Vec::<(LinkId, Cid)>::default(),
                    ),
                    EDBFact::new(
                        "evac",
                        [
                            ("entity", Datum::int(3)),
                            ("attribute", Datum::string("to")),
                            ("value", Datum::int(4)),
                        ],
                        Vec::<(LinkId, Cid)>::default(),
                    ),
                ]))
            })
            .unwrap();

        reactor
            .register_sink("path", move || {
                Box::new(unfold((), move |_, fact| async move {
                    println!("{fact}");

                    Ok(())
                }))
            })
            .unwrap();

        reactor
            .register_sink("edge", move || {
                Box::new(unfold((), move |_, fact| async move {
                    println!("{fact}");

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

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use pretty_assertions::assert_eq;
    use std::{
        cell::RefCell,
        collections::BTreeSet,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use futures::{channel::oneshot, sink::unfold, stream};
    use tokio::{spawn, test, time::timeout};

    use rhizome::{
        builder::ProgramBuilder,
        fact::traits::{EDBFact, IDBFact},
        logic::lower_to_ram,
        reactor::Reactor,
        vm::VM,
    };

    #[test]
    async fn test_sink_transitive_closure() -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let buf1 = Arc::new(Mutex::new(RefCell::new(BTreeSet::new())));
        let buf2 = Arc::clone(&buf1);

        spawn(async move {
            //     let program = parser::parse(
            //         r#"
            // input evac(entity, attribute, value).

            // output edge(from, to).
            // output path(from, to).

            // edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

            // path(from: X, to: Y) :- edge(from: X, to: Y).
            // path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
            // "#,
            //     )
            //     .unwrap();

            let program = ProgramBuilder::build(|p| {
                p.input("evac", |h| {
                    h.column::<i32>("entity")?
                        .column::<&str>("attribute")?
                        .column::<i32>("value")
                })?;

                p.output("edge", |h| h.column::<i32>("from")?.column::<i32>("to"))?;
                p.output("path", |h| h.column::<i32>("from")?.column::<i32>("to"))?;

                p.rule(
                    "edge",
                    |h| h.bind("from", "x")?.bind("to", "y"),
                    |b| {
                        b.search("evac", |s| {
                            s.bind("entity", "x")?
                                .bind("value", "y")?
                                .when("attribute", "to")
                        })
                    },
                )?;

                p.rule(
                    "path",
                    |h| h.bind("from", "x")?.bind("to", "y"),
                    |b| b.search("edge", |s| s.bind("from", "x")?.bind("to", "y")),
                )?;

                p.rule(
                    "path",
                    |h| h.bind("from", "x")?.bind("to", "z"),
                    |b| {
                        b.search("edge", |s| s.bind("from", "x")?.bind("to", "y"))?
                            .search("path", |s| s.bind("from", "y")?.bind("to", "z"))
                    },
                )
            })
            .unwrap();

            let ast = lower_to_ram::lower_to_ram(&program)?;
            let (_, mut reactor): (_, Reactor) = Reactor::new(VM::new(ast));

            reactor.register_stream(|| {
                Box::new(stream::iter([
                    EDBFact::new(0, "to", 1, []),
                    EDBFact::new(1, "to", 2, []),
                    EDBFact::new(2, "to", 3, []),
                    EDBFact::new(3, "to", 4, []),
                ]))
            })?;

            reactor.register_sink("path", || {
                Box::new(unfold((), move |_, fact| {
                    let b = Arc::clone(&buf1);
                    async move {
                        Arc::clone(&b).lock().unwrap().borrow_mut().insert(fact);
                        Ok(())
                    }
                }))
            })?;

            reactor.subscribe(tx);

            reactor.async_run().await
        });

        timeout(Duration::from_secs(1), rx)
            .await
            .expect("run timed out.")
            .expect("run errored.");

        assert_eq!(
            *buf2.lock().unwrap().borrow(),
            BTreeSet::from_iter([
                IDBFact::new("path", [("from", 0), ("to", 1)]),
                IDBFact::new("path", [("from", 0), ("to", 2)]),
                IDBFact::new("path", [("from", 0), ("to", 3)]),
                IDBFact::new("path", [("from", 0), ("to", 4)]),
                IDBFact::new("path", [("from", 1), ("to", 2)]),
                IDBFact::new("path", [("from", 1), ("to", 3)]),
                IDBFact::new("path", [("from", 1), ("to", 4)]),
                IDBFact::new("path", [("from", 2), ("to", 3)]),
                IDBFact::new("path", [("from", 2), ("to", 4)]),
                IDBFact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }
}

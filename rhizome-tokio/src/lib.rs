#[cfg(test)]
mod tests {
    use cid::Cid;
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
        datum::Datum,
        fact::traits::{EDBFact, IDBFact},
        id::LinkId,
        logic::{lower_to_ram, parser},
        ram::vm::VM,
        reactor::Reactor,
    };

    #[test]
    async fn test_sink_transitive_closure() {
        let (tx, rx) = oneshot::channel();

        let buf1 = Arc::new(Mutex::new(RefCell::new(BTreeSet::new())));
        let buf2 = Arc::clone(&buf1);

        spawn(async move {
            let program = parser::parse(
                r#"
        input evac(entity, attribute, value).

        output edge(from, to).
        output path(from, to).

        edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
            )
            .unwrap();

            let ast = lower_to_ram::lower_to_ram(&program).unwrap();
            let mut reactor: Reactor = Reactor::new(VM::new(ast));

            reactor
                .register_stream(|| {
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
                .register_sink("path", || {
                    Box::new(unfold((), move |_, fact| {
                        let b = Arc::clone(&buf1);
                        async move {
                            Arc::clone(&b).lock().unwrap().borrow_mut().insert(fact);
                            Ok(())
                        }
                    }))
                })
                .unwrap();

            reactor.subscribe(tx);

            reactor.async_run().await.unwrap();
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
    }
}

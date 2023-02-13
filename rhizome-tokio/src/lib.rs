#[cfg(test)]
mod tests {
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
        fact::Fact,
        logic::{lower_to_ram, parser},
        ram::vm::VM,
        reactor::Reactor,
    };

    #[test]
    async fn test_sink_transitive_closure() {
        let (tx, rx) = oneshot::channel();

        let buf1 = Arc::new(Mutex::new(RefCell::new(BTreeSet::<Fact>::new())));
        let buf2 = Arc::clone(&buf1);

        spawn(async move {
            let program = parser::parse(
                r#"
        input edge(from, to).

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
                        Fact::new("edge", [("from", 0), ("to", 1)]),
                        Fact::new("edge", [("from", 1), ("to", 2)]),
                        Fact::new("edge", [("from", 2), ("to", 3)]),
                        Fact::new("edge", [("from", 3), ("to", 4)]),
                    ]))
                })
                .unwrap();

            let sink = unfold((), move |_, fact: Fact| {
                let b = Arc::clone(&buf1);
                async move {
                    Arc::clone(&b).lock().unwrap().borrow_mut().insert(fact);
                    Ok(())
                }
            });

            reactor.register_sink("path", || Box::new(sink)).unwrap();
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
                Fact::new("path", [("from", 0), ("to", 1)]),
                Fact::new("path", [("from", 0), ("to", 2)]),
                Fact::new("path", [("from", 0), ("to", 3)]),
                Fact::new("path", [("from", 0), ("to", 4)]),
                Fact::new("path", [("from", 1), ("to", 2)]),
                Fact::new("path", [("from", 1), ("to", 3)]),
                Fact::new("path", [("from", 1), ("to", 4)]),
                Fact::new("path", [("from", 2), ("to", 3)]),
                Fact::new("path", [("from", 2), ("to", 4)]),
                Fact::new("path", [("from", 3), ("to", 4)]),
            ])
        );
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use pretty_assertions::assert_eq;
    use std::{
        cell::RefCell,
        collections::BTreeSet,
        sync::{Arc, Mutex},
    };

    use futures::{sink::unfold, StreamExt};
    use tokio::{spawn, test};

    use rhizome::{
        runtime::client::Client,
        tuple::{InputTuple, Tuple},
    };

    #[test]
    async fn test_sink_transitive_closure() -> Result<()> {
        let buf1 = Arc::new(Mutex::new(RefCell::new(BTreeSet::new())));
        let buf2 = Arc::clone(&buf1);

        let (mut client, mut rx, reactor) = Client::new();

        spawn(async move {
            reactor
                .async_run(|p| {
                    p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
                    p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

                    p.rule::<(i32, i32)>("edge", &|h, b, (x, y)| {
                        h.bind((("from", x), ("to", y)))?;
                        b.search("evac", (("entity", x), ("attribute", "to"), ("value", y)))?;

                        Ok(())
                    })?;

                    p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                        h.bind((("from", x), ("to", y)))?;
                        b.search("edge", (("from", x), ("to", y)))?;

                        Ok(())
                    })?;

                    p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                        h.bind((("from", x), ("to", z)))?;

                        b.search("edge", (("from", x), ("to", y)))?;
                        b.search("path", (("from", y), ("to", z)))?;

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
                "path",
                Box::new(|| {
                    Box::new(unfold((), move |(), fact| {
                        let b = Arc::clone(&buf1);
                        async move {
                            Arc::clone(&b).lock().unwrap().borrow_mut().insert(fact);
                            Ok(())
                        }
                    }))
                }),
            )
            .await?;

        client
            .insert_fact(InputTuple::new(0, "to", 1, vec![]))
            .await?;
        client
            .insert_fact(InputTuple::new(1, "to", 2, vec![]))
            .await?;
        client
            .insert_fact(InputTuple::new(2, "to", 3, vec![]))
            .await?;
        client
            .insert_fact(InputTuple::new(3, "to", 4, vec![]))
            .await?;
        client.flush().await?;

        assert_eq!(
            *buf2.lock().unwrap().borrow(),
            BTreeSet::from_iter([
                Tuple::new("path", [("from", 0), ("to", 1)], None),
                Tuple::new("path", [("from", 0), ("to", 2)], None),
                Tuple::new("path", [("from", 0), ("to", 3)], None),
                Tuple::new("path", [("from", 0), ("to", 4)], None),
                Tuple::new("path", [("from", 1), ("to", 2)], None),
                Tuple::new("path", [("from", 1), ("to", 3)], None),
                Tuple::new("path", [("from", 1), ("to", 4)], None),
                Tuple::new("path", [("from", 2), ("to", 3)], None),
                Tuple::new("path", [("from", 2), ("to", 4)], None),
                Tuple::new("path", [("from", 3), ("to", 4)], None),
            ])
        );

        Ok(())
    }
}

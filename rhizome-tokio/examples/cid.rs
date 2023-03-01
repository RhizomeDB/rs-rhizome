use anyhow::Result;
use cid::Cid;
use futures::{channel::oneshot, sink::unfold, stream};
use rhizome::{
    datum::Datum,
    fact::{evac_fact::EVACFact, traits::EDBFact},
    id::LinkId,
    logic::parser,
    storage::{content_addressable::ContentAddressable, DefaultCodec},
};
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = oneshot::channel();

    spawn(async move {
        let program = parser::parse(
            r#"
            input evac(cid, entity, attribute, value).

            output create(cid, entity, initial).
            output update(cid, entity, parent, value).
            output head(cid, entity, value).

            create(cid: Cid, entity: E, initial: I) :-
                evac(cid: Cid, entity: E, attribute: "initial", value: I).

            update(cid: Cid, entity: E, parent: P, value: V) :-
                evac(cid: Cid, entity: E, attribute: "write", value: V),
                links Cid (parent: P),
                create(cid: P, entity: E).

            update(cid: Cid, entity: E, parent: P, value: V) :-
                evac(cid: Cid, entity: E, attribute: "write", value: V),
                links Cid (parent: P),
                update(cid: P, entity: E).

            head(cid: Cid, entity: E, value: V) :-
                create(cid: Cid, entity: E, initial: V),
                !update(entity: E).

            head(cid: Cid, entity: E, value: V) :-
                update(cid: Cid, entity: E, value: V),
                !update(entity: E, parent: Cid).
            "#,
        )
        .unwrap();

        // let ram = logic::lower_to_ram::lower_to_ram(&program).unwrap();

        // let mut buf = Vec::<u8>::new();
        // ram.to_doc().render(80, &mut buf).unwrap();

        // println!("{}", String::from_utf8(buf).unwrap());

        let e0 = EVACFact::new(
            "evac",
            [
                ("entity", Datum::int(0)),
                ("attribute", Datum::string("initial")),
                ("value", Datum::int(0)),
            ],
            Vec::<(LinkId, Cid)>::default(),
        );

        let e1 = EVACFact::new(
            "evac",
            [
                ("entity", Datum::int(0)),
                ("attribute", Datum::string("write")),
                ("value", Datum::int(1)),
            ],
            [("parent", e0.cid(DefaultCodec::default()))],
        );

        let e2 = EVACFact::new(
            "evac",
            [
                ("entity", Datum::int(0)),
                ("attribute", Datum::string("write")),
                ("value", Datum::int(5)),
            ],
            [("parent", e1.cid(DefaultCodec::default()))],
        );

        let e3 = EVACFact::new(
            "evac",
            [
                ("entity", Datum::int(0)),
                ("attribute", Datum::string("write")),
                ("value", Datum::int(3)),
            ],
            [("parent", e1.cid(DefaultCodec::default()))],
        );

        let e4 = EVACFact::new(
            "evac",
            [
                ("entity", Datum::int(1)),
                ("attribute", Datum::string("initial")),
                ("value", Datum::int(4)),
            ],
            Vec::<(LinkId, Cid)>::default(),
        );

        let mut reactor = rhizome::spawn(&program).unwrap();
        reactor
            .register_stream(move || Box::new(stream::iter([e0, e1, e2, e3, e4])))
            .unwrap();

        reactor
            .register_sink("create", move || {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

                    Ok(())
                }))
            })
            .unwrap();

        reactor
            .register_sink("update", move || {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

                    Ok(())
                }))
            })
            .unwrap();

        reactor
            .register_sink("head", move || {
                Box::new(unfold((), move |_, fact| async move {
                    println!("Derived: {fact}");

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

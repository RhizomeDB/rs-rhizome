use std::fmt;
use std::fmt::Debug;

use futures::{channel::oneshot, Sink, Stream};
use rhizome_runtime::MaybeSend;

use crate::{
    error::Error,
    fact::traits::{EDBFact, IDBFact},
    id::RelationId,
    timestamp::Timestamp,
};

pub mod client;

mod reactor;
mod vm;

pub type FactStream<F> = Box<dyn Stream<Item = F>>;
pub type FactSink<F> = Box<dyn Sink<F, Error = Error>>;

pub trait CreateStream<T>: (FnOnce() -> FactStream<T>) + MaybeSend {}
pub trait CreateSink<T>: (FnOnce() -> FactSink<T>) + MaybeSend {}

impl<F, T> CreateStream<T> for F
where
    F: FnOnce() -> FactStream<T> + MaybeSend,
    T: EDBFact,
{
}

impl<F, T> CreateSink<T> for F
where
    F: FnOnce() -> FactSink<T> + MaybeSend,
    T: IDBFact,
{
}

#[derive(Debug)]
pub enum StreamEvent<T> {
    Fact(T),
}

#[derive(Debug)]
pub enum SinkCommand<T> {
    Flush(oneshot::Sender<()>),
    ProcessFact(T),
}

#[derive(Debug)]
pub enum ClientEvent<T>
where
    T: Timestamp,
{
    ReachedFixedpoint(T),
}

pub enum ClientCommand<E, I>
where
    E: EDBFact,
    I: IDBFact,
{
    Flush(oneshot::Sender<()>),
    InsertFact(E, oneshot::Sender<()>),
    RegisterStream(RelationId, Box<dyn CreateStream<E>>, oneshot::Sender<()>),
    RegisterSink(RelationId, Box<dyn CreateSink<I>>, oneshot::Sender<()>),
}

impl<E, I> Debug for ClientCommand<E, I>
where
    E: EDBFact,
    I: IDBFact,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientCommand::Flush(_) => f.debug_tuple("Flush").finish(),
            ClientCommand::InsertFact(fact, _) => f.debug_tuple("InsertFact").field(fact).finish(),
            ClientCommand::RegisterStream(_, _, _) => f.debug_tuple("RegisterStream").finish(),
            ClientCommand::RegisterSink(_, _, _) => f.debug_tuple("RegisterSink").finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{assert_derives, fact::traits::IDBFact, types::Any};

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() {
        assert_derives!(
            "path",
            |p| {
                p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
                p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

                p.fact("edge", |f| f.bind((("from", 0), ("to", 1))))?;
                p.fact("edge", |f| f.bind((("from", 1), ("to", 2))))?;
                p.fact("edge", |f| f.bind((("from", 2), ("to", 3))))?;
                p.fact("edge", |f| f.bind((("from", 3), ("to", 4))))?;

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
            },
            [
                IDBFact::new("path", [("from", 0), ("to", 1)],),
                IDBFact::new("path", [("from", 0), ("to", 2)],),
                IDBFact::new("path", [("from", 0), ("to", 3)],),
                IDBFact::new("path", [("from", 0), ("to", 4)],),
                IDBFact::new("path", [("from", 1), ("to", 2)],),
                IDBFact::new("path", [("from", 1), ("to", 3)],),
                IDBFact::new("path", [("from", 1), ("to", 4)],),
                IDBFact::new("path", [("from", 2), ("to", 3)],),
                IDBFact::new("path", [("from", 2), ("to", 4)],),
                IDBFact::new("path", [("from", 3), ("to", 4)],),
            ]
        );
    }

    #[test]
    fn test_source_transitive_closure() {
        assert_derives!(
            "path",
            |p| {
                p.input("evac", |h| {
                    h.column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
                p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

                p.fact("edge", |f| f.bind((("from", 0), ("to", 1))))?;
                p.fact("edge", |f| f.bind((("from", 1), ("to", 2))))?;
                p.fact("edge", |f| f.bind((("from", 2), ("to", 3))))?;
                p.fact("edge", |f| f.bind((("from", 3), ("to", 4))))?;

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
            },
            [
                EDBFact::new(0, "to", 1, vec![]),
                EDBFact::new(1, "to", 2, vec![]),
                EDBFact::new(2, "to", 3, vec![]),
                EDBFact::new(3, "to", 4, vec![]),
            ],
            [
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
            ]
        );
    }
}

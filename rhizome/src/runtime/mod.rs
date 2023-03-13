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

    use cid::Cid;

    use crate::{
        assert_derives,
        fact::{btree_fact::BTreeFact, evac_fact::EVACFact, traits::IDBFact},
        types::Any,
        value::Val,
    };

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() {
        assert_derives!(
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
            [(
                "path",
                [
                    BTreeFact::new("path", [("from", 0), ("to", 1)],),
                    BTreeFact::new("path", [("from", 0), ("to", 2)],),
                    BTreeFact::new("path", [("from", 0), ("to", 3)],),
                    BTreeFact::new("path", [("from", 0), ("to", 4)],),
                    BTreeFact::new("path", [("from", 1), ("to", 2)],),
                    BTreeFact::new("path", [("from", 1), ("to", 3)],),
                    BTreeFact::new("path", [("from", 1), ("to", 4)],),
                    BTreeFact::new("path", [("from", 2), ("to", 3)],),
                    BTreeFact::new("path", [("from", 2), ("to", 4)],),
                    BTreeFact::new("path", [("from", 3), ("to", 4)],),
                ]
            )]
        );
    }

    #[test]
    fn test_source_transitive_closure() {
        assert_derives!(
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
                EVACFact::new(0, "to", 1, vec![]),
                EVACFact::new(1, "to", 2, vec![]),
                EVACFact::new(2, "to", 3, vec![]),
                EVACFact::new(3, "to", 4, vec![]),
            ],
            [(
                "path",
                [
                    BTreeFact::new("path", [("from", 0), ("to", 1)]),
                    BTreeFact::new("path", [("from", 0), ("to", 2)]),
                    BTreeFact::new("path", [("from", 0), ("to", 3)]),
                    BTreeFact::new("path", [("from", 0), ("to", 4)]),
                    BTreeFact::new("path", [("from", 1), ("to", 2)]),
                    BTreeFact::new("path", [("from", 1), ("to", 3)]),
                    BTreeFact::new("path", [("from", 1), ("to", 4)]),
                    BTreeFact::new("path", [("from", 2), ("to", 3)]),
                    BTreeFact::new("path", [("from", 2), ("to", 4)]),
                    BTreeFact::new("path", [("from", 3), ("to", 4)]),
                ]
            )]
        );
    }

    #[test]
    fn test_get_link() {
        let f00 = EVACFact::new(0, "node", 0, vec![]);
        let f01 = EVACFact::new(0, "node", 0, vec![("parent", f00.cid())]);
        let f02 = EVACFact::new(0, "node", 0, vec![("parent", f01.cid())]);
        let f03 = EVACFact::new(0, "node", 0, vec![("parent", f02.cid())]);
        let f04 = EVACFact::new(0, "node", 1, vec![("parent", f02.cid())]);
        let f10 = EVACFact::new(1, "node", 0, vec![("parent", f00.cid())]);
        let f11 = EVACFact::new(1, "node", 0, vec![("parent", f10.cid())]);
        let f12 = EVACFact::new(1, "node", 0, vec![("parent", f11.cid())]);

        let idb = [
            (
                "root",
                vec![
                    BTreeFact::new("root", [("tree", Val::S32(0)), ("id", f00.cid())]),
                    BTreeFact::new("root", [("tree", Val::S32(1)), ("id", f10.cid())]),
                ],
            ),
            (
                "parent",
                vec![
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", f00.cid()),
                            ("child", f01.cid()),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", f01.cid()),
                            ("child", f02.cid()),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", f02.cid()),
                            ("child", f03.cid()),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", f02.cid()),
                            ("child", f04.cid()),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", f10.cid()),
                            ("child", f11.cid()),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", f11.cid()),
                            ("child", f12.cid()),
                        ],
                    ),
                ],
            ),
        ];

        assert_derives!(
            |p| {
                p.input("evac", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("parent", |h| {
                    h.column::<i32>("tree")
                        .column::<Cid>("parent")
                        .column::<Cid>("child")
                })?;

                p.output("root", |h| h.column::<i32>("tree").column::<Cid>("id"))?;

                p.rule::<(i32, Cid, Cid)>("parent", &|h, b, (tree, parent, child)| {
                    (
                        h.bind((("tree", tree), ("parent", parent), ("child", child))),
                        b.search("evac", (("cid", parent), ("entity", tree)))
                            .search("evac", (("cid", child), ("entity", tree)))
                            .get_link(child, "parent", parent),
                    )
                })?;

                p.rule::<(i32, Cid)>("root", &|h, b, (tree, root)| {
                    (
                        h.bind((("tree", tree), ("id", root))),
                        b.search("evac", (("cid", root), ("entity", tree)))
                            .except("parent", (("child", root), ("tree", tree))),
                    )
                })
            },
            [f00, f01, f02, f03, f04, f10, f11, f12,],
            idb
        );
    }
}

use std::{fmt, fmt::Debug};

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

    use std::cmp;

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
                    BTreeFact::new("root", [("tree", Val::S32(0)), ("id", Val::Cid(f00.cid()))]),
                    BTreeFact::new("root", [("tree", Val::S32(1)), ("id", Val::Cid(f10.cid()))]),
                ],
            ),
            (
                "parent",
                vec![
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f00.cid())),
                            ("child", Val::Cid(f01.cid())),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f01.cid())),
                            ("child", Val::Cid(f02.cid())),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f02.cid())),
                            ("child", Val::Cid(f03.cid())),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f02.cid())),
                            ("child", Val::Cid(f04.cid())),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", Val::Cid(f10.cid())),
                            ("child", Val::Cid(f11.cid())),
                        ],
                    ),
                    BTreeFact::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", Val::Cid(f11.cid())),
                            ("child", Val::Cid(f12.cid())),
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

    #[test]
    fn test_user_defined_predicate() {
        assert_derives!(
            |p| {
                p.input("evac", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("triangle", |h| {
                    h.column::<i32>("a").column::<i32>("b").column::<i32>("c")
                })?;

                p.rule::<(i32, i32, i32)>("triangle", &|h, b, (x, y, z)| {
                    (
                        h.bind((("a", x), ("b", y), ("c", z))),
                        b.search("evac", (("value", x),))
                            .search("evac", (("value", y),))
                            .search("evac", (("value", z),))
                            .predicate((x, y, z), |(x, y, z)| x + y < z),
                    )
                })
            },
            [
                EVACFact::new(0, "n", 1, vec![]),
                EVACFact::new(0, "n", 2, vec![]),
                EVACFact::new(0, "n", 3, vec![]),
                EVACFact::new(0, "n", 4, vec![]),
                EVACFact::new(0, "n", 5, vec![]),
            ],
            [(
                "triangle",
                [
                    BTreeFact::new("triangle", [("a", 1), ("b", 1), ("c", 3)],),
                    BTreeFact::new("triangle", [("a", 1), ("b", 1), ("c", 4)],),
                    BTreeFact::new("triangle", [("a", 1), ("b", 1), ("c", 5)],),
                    BTreeFact::new("triangle", [("a", 1), ("b", 2), ("c", 4)],),
                    BTreeFact::new("triangle", [("a", 1), ("b", 2), ("c", 5)],),
                    BTreeFact::new("triangle", [("a", 1), ("b", 3), ("c", 5)],),
                    BTreeFact::new("triangle", [("a", 2), ("b", 1), ("c", 4)],),
                    BTreeFact::new("triangle", [("a", 2), ("b", 1), ("c", 5)],),
                    BTreeFact::new("triangle", [("a", 2), ("b", 2), ("c", 5)],),
                    BTreeFact::new("triangle", [("a", 3), ("b", 1), ("c", 5)],),
                ]
            )]
        );
    }

    #[test]
    fn test_reduce() {
        assert_derives!(
            |p| {
                p.input("evac", |h| {
                    h.column::<Cid>("cid")
                        .column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("pair", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.output("count", |h| h.column::<i32>("n"))?;
                p.output("sum", |h| h.column::<i32>("n"))?;
                p.output("min", |h| h.column::<i32>("n"))?;
                p.output("max", |h| h.column::<i32>("n"))?;

                p.output("product", |h| h.column::<i32>("z"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    (h.bind((("n", x),)), b.search("evac", (("value", x),)))
                })?;

                p.rule::<(i32, i32)>("count", &|h, b, (count, n)| {
                    (
                        h.bind((("n", count),)),
                        b.reduce(count, (n,), "num", (("n", n),), 0, |acc, (_,)| acc + 1),
                    )
                })?;

                p.rule::<(i32, i32)>("sum", &|h, b, (sum, n)| {
                    (
                        h.bind((("n", sum),)),
                        b.reduce(sum, (n,), "num", (("n", n),), 0, |acc, (x,)| acc + x),
                    )
                })?;

                p.rule::<(i32, i32)>("min", &|h, b, (min, n)| {
                    (
                        h.bind((("n", min),)),
                        b.reduce(min, (n,), "num", (("n", n),), i32::MAX, |acc, (x,)| {
                            cmp::min(acc, x)
                        }),
                    )
                })?;

                p.rule::<(i32, i32)>("max", &|h, b, (max, n)| {
                    (
                        h.bind((("n", max),)),
                        b.reduce(max, (n,), "num", (("n", n),), i32::MIN, |acc, (x,)| {
                            cmp::max(acc, x)
                        }),
                    )
                })
            },
            [
                EVACFact::new(0, "n", 1, vec![]),
                EVACFact::new(0, "n", 2, vec![]),
                EVACFact::new(0, "n", 3, vec![]),
                EVACFact::new(0, "n", 4, vec![]),
                EVACFact::new(0, "n", 5, vec![]),
            ],
            [
                ("count", [BTreeFact::new("count", [("n", 5),],),]),
                ("sum", [BTreeFact::new("sum", [("n", 15),],),]),
                ("min", [BTreeFact::new("min", [("n", 1),],),]),
                ("max", [BTreeFact::new("max", [("n", 5),],),]),
            ]
        );
    }

    #[test]
    fn test_multi_arity_reduce() {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("pair", |h| h.column::<i32>("x").column::<i32>("y"))?;
                p.output("product", |h| h.column::<i32>("z"))?;

                p.fact("num", |f| f.bind((("n", 1),)))?;
                p.fact("num", |f| f.bind((("n", 2),)))?;
                p.fact("num", |f| f.bind((("n", 3),)))?;
                p.fact("num", |f| f.bind((("n", 4),)))?;
                p.fact("num", |f| f.bind((("n", 5),)))?;

                p.rule::<(i32, i32)>("pair", &|h, b, (x, y)| {
                    (
                        h.bind((("x", x), ("y", y))),
                        b.search("num", (("n", x),)).search("num", (("n", y),)),
                    )
                })?;

                p.rule::<(i32, i32, i32)>("product", &|h, b, (x, y, z)| {
                    (
                        h.bind((("z", z),)),
                        b.reduce(z, (x, y), "pair", (("x", x), ("y", y)), 0, |acc, (x, y)| {
                            acc + x * y
                        }),
                    )
                })
            },
            [("product", [BTreeFact::new("product", [("z", 225),],),]),]
        );
    }

    #[test]
    fn test_group_by_reduce() {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("pair", |h| h.column::<i32>("x").column::<i32>("y"))?;
                p.output("product", |h| {
                    h.column::<i32>("x").column::<i32>("y").column::<i32>("z")
                })?;

                p.fact("num", |f| f.bind((("n", 1),)))?;
                p.fact("num", |f| f.bind((("n", 2),)))?;
                p.fact("num", |f| f.bind((("n", 3),)))?;
                p.fact("num", |f| f.bind((("n", 4),)))?;
                p.fact("num", |f| f.bind((("n", 5),)))?;

                p.rule::<(i32, i32)>("pair", &|h, b, (x, y)| {
                    (
                        h.bind((("x", x), ("y", y))),
                        b.search("num", (("n", x),)).search("num", (("n", y),)),
                    )
                })?;

                p.rule::<(i32, i32, i32)>("product", &|h, b, (x, y, z)| {
                    (
                        h.bind((("x", x), ("y", y), ("z", z))),
                        b.search("pair", (("x", x), ("y", y))).reduce(
                            z,
                            (x, y),
                            "pair",
                            (("x", x), ("y", y)),
                            0,
                            |_, (x, y)| x * y,
                        ),
                    )
                })
            },
            [(
                "product",
                [
                    BTreeFact::new("product", [("x", 1), ("y", 1), ("z", 1),],),
                    BTreeFact::new("product", [("x", 1), ("y", 2), ("z", 2),],),
                    BTreeFact::new("product", [("x", 1), ("y", 3), ("z", 3),],),
                    BTreeFact::new("product", [("x", 1), ("y", 4), ("z", 4),],),
                    BTreeFact::new("product", [("x", 1), ("y", 5), ("z", 5),],),
                    BTreeFact::new("product", [("x", 2), ("y", 1), ("z", 2),],),
                    BTreeFact::new("product", [("x", 2), ("y", 2), ("z", 4),],),
                    BTreeFact::new("product", [("x", 2), ("y", 3), ("z", 6),],),
                    BTreeFact::new("product", [("x", 2), ("y", 4), ("z", 8),],),
                    BTreeFact::new("product", [("x", 2), ("y", 5), ("z", 10),],),
                    BTreeFact::new("product", [("x", 3), ("y", 1), ("z", 3),],),
                    BTreeFact::new("product", [("x", 3), ("y", 2), ("z", 6),],),
                    BTreeFact::new("product", [("x", 3), ("y", 3), ("z", 9),],),
                    BTreeFact::new("product", [("x", 3), ("y", 4), ("z", 12),],),
                    BTreeFact::new("product", [("x", 3), ("y", 5), ("z", 15),],),
                    BTreeFact::new("product", [("x", 4), ("y", 1), ("z", 4),],),
                    BTreeFact::new("product", [("x", 4), ("y", 2), ("z", 8),],),
                    BTreeFact::new("product", [("x", 4), ("y", 3), ("z", 12),],),
                    BTreeFact::new("product", [("x", 4), ("y", 4), ("z", 16),],),
                    BTreeFact::new("product", [("x", 4), ("y", 5), ("z", 20),],),
                    BTreeFact::new("product", [("x", 5), ("y", 1), ("z", 5),],),
                    BTreeFact::new("product", [("x", 5), ("y", 2), ("z", 10),],),
                    BTreeFact::new("product", [("x", 5), ("y", 3), ("z", 15),],),
                    BTreeFact::new("product", [("x", 5), ("y", 4), ("z", 20),],),
                    BTreeFact::new("product", [("x", 5), ("y", 5), ("z", 25),],),
                ]
            ),]
        );
    }
}
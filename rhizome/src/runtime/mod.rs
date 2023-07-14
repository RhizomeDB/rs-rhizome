use std::{fmt, fmt::Debug};

use cid::Cid;
use futures::{channel::oneshot, Sink, Stream};
use rhizome_runtime::MaybeSend;

use crate::{
    error::Error,
    id::RelationId,
    timestamp::Timestamp,
    tuple::{InputTuple, Tuple},
};

pub mod client;
pub mod epoch;
pub mod reactor;
mod vm;

pub type TupleStream = Box<dyn Stream<Item = InputTuple>>;
pub type TupleSink = Box<dyn Sink<Tuple, Error = Error>>;

pub trait CreateStream: (FnOnce() -> TupleStream) + MaybeSend {}
pub trait CreateSink: (FnOnce() -> TupleSink) + MaybeSend {}

impl<F> CreateStream for F where F: FnOnce() -> TupleStream + MaybeSend {}

impl<F> CreateSink for F where F: FnOnce() -> TupleSink + MaybeSend {}

#[derive(Debug)]
pub enum StreamEvent {
    Tuple(InputTuple),
}

#[derive(Debug)]
pub enum SinkCommand {
    Flush(oneshot::Sender<()>),
    ProcessTuple(Tuple),
}

#[derive(Debug)]
pub enum ClientEvent<T>
where
    T: Timestamp,
{
    ReachedFixedpoint(T, Cid),
}

pub enum ClientCommand {
    Flush(oneshot::Sender<()>),
    InsertTuple(Box<InputTuple>, oneshot::Sender<()>),
    RegisterStream(RelationId, Box<dyn CreateStream>, oneshot::Sender<()>),
    RegisterSink(RelationId, Box<dyn CreateSink>, oneshot::Sender<()>),
}

impl Debug for ClientCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientCommand::Flush(_) => f.debug_tuple("Flush").finish(),
            ClientCommand::InsertTuple(tuple, _) => {
                f.debug_tuple("InsertTuple").field(tuple).finish()
            }
            ClientCommand::RegisterStream(_, _, _) => f.debug_tuple("RegisterStream").finish(),
            ClientCommand::RegisterSink(_, _, _) => f.debug_tuple("RegisterSink").finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        marker::PhantomData,
        ops::{Add, AddAssign},
    };

    use anyhow::Result;
    use cid::Cid;
    use num_traits::{WrappingMul, Zero};
    use rhizome_macro::rhizome_fn;

    use crate::{
        aggregation::Aggregate,
        assert_derives,
        kernel::{self, math},
        predicate::Predicate,
        types::RhizomeType,
        value::{Any, Val},
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
            },
            [(
                "path",
                [
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
                ]
            )]
        );
    }

    #[test]
    fn test_source_transitive_closure() -> Result<()> {
        assert_derives!(
            |p| {
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
            },
            [
                InputTuple::new(0, "to", 1, []),
                InputTuple::new(1, "to", 2, []),
                InputTuple::new(2, "to", 3, []),
                InputTuple::new(3, "to", 4, []),
            ],
            [(
                "path",
                [
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
                ]
            )]
        );

        Ok(())
    }

    #[test]
    fn test_negate_edb() -> Result<()> {
        // Attach a link to one fact to ensure negation works against facts with links
        let cid = Cid::try_from("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")?;

        assert_derives!(
            |p| {
                p.output("result", |h| {
                    h.column::<i32>("entity").column::<i32>("value")
                })?;

                p.rule::<(i32, i32)>("result", &|h, b, (e, v)| {
                    h.bind((("entity", e), ("value", v)))?;

                    b.search(
                        "evac",
                        (("entity", e), ("attribute", "value"), ("value", v)),
                    )?;
                    b.except(
                        "evac",
                        (("entity", e), ("attribute", "ignored"), ("value", true)),
                    )?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "value", 0, []),
                InputTuple::new(0, "value", 1, []),
                InputTuple::new(1, "value", 2, []),
                InputTuple::new(2, "value", 2, []),
                InputTuple::new(3, "value", 23, []),
                InputTuple::new(1, "ignored", true, []),
                InputTuple::new(2, "ignored", false, []),
                InputTuple::new(3, "ignored", true, [cid]),
            ],
            [(
                "result",
                [
                    Tuple::new("result", [("entity", 0), ("value", 0)], None),
                    Tuple::new("result", [("entity", 0), ("value", 1)], None),
                    Tuple::new("result", [("entity", 2), ("value", 2)], None),
                ]
            )]
        );

        Ok(())
    }

    #[test]
    fn test_negate_idb() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("ignored", |h| h.column::<i32>("entity"))?;

                p.output("result", |h| {
                    h.column::<i32>("entity").column::<i32>("value")
                })?;

                p.fact("ignored", |h| h.bind((("entity", 1),)))?;

                p.rule::<(i32, i32)>("result", &|h, b, (e, v)| {
                    h.bind((("entity", e), ("value", v)))?;

                    b.search(
                        "evac",
                        (("entity", e), ("attribute", "value"), ("value", v)),
                    )?;
                    b.except("ignored", (("entity", e),))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "value", 0, []),
                InputTuple::new(0, "value", 1, []),
                InputTuple::new(1, "value", 2, []),
                InputTuple::new(2, "value", 2, []),
            ],
            [(
                "result",
                [
                    Tuple::new("result", [("entity", 0), ("value", 0)], None),
                    Tuple::new("result", [("entity", 0), ("value", 1)], None),
                    Tuple::new("result", [("entity", 2), ("value", 2)], None),
                ]
            )]
        );

        Ok(())
    }

    #[test]
    fn test_get_link() -> Result<()> {
        let f00 = InputTuple::new(0, "node", 0, []);
        let f01 = InputTuple::new(0, "node", 0, [f00.cid()?]);
        let f02 = InputTuple::new(0, "node", 0, [f01.cid()?]);
        let f03 = InputTuple::new(0, "node", 0, [f02.cid()?]);
        let f04 = InputTuple::new(0, "node", 1, [f02.cid()?]);
        let f10 = InputTuple::new(1, "node", 0, [f00.cid()?]);
        let f11 = InputTuple::new(1, "node", 0, [f10.cid()?]);
        let f12 = InputTuple::new(1, "node", 0, [f11.cid()?]);

        let idb = [
            (
                "root",
                vec![
                    Tuple::new(
                        "root",
                        [("tree", Val::S32(0)), ("id", Val::Cid(f00.cid()?))],
                        None,
                    ),
                    Tuple::new(
                        "root",
                        [("tree", Val::S32(1)), ("id", Val::Cid(f10.cid()?))],
                        None,
                    ),
                ],
            ),
            (
                "parent",
                vec![
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f00.cid()?)),
                            ("child", Val::Cid(f01.cid()?)),
                        ],
                        None,
                    ),
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f01.cid()?)),
                            ("child", Val::Cid(f02.cid()?)),
                        ],
                        None,
                    ),
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f02.cid()?)),
                            ("child", Val::Cid(f03.cid()?)),
                        ],
                        None,
                    ),
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(0)),
                            ("parent", Val::Cid(f02.cid()?)),
                            ("child", Val::Cid(f04.cid()?)),
                        ],
                        None,
                    ),
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", Val::Cid(f10.cid()?)),
                            ("child", Val::Cid(f11.cid()?)),
                        ],
                        None,
                    ),
                    Tuple::new(
                        "parent",
                        [
                            ("tree", Val::S32(1)),
                            ("parent", Val::Cid(f11.cid()?)),
                            ("child", Val::Cid(f12.cid()?)),
                        ],
                        None,
                    ),
                ],
            ),
        ];

        assert_derives!(
            |p| {
                p.output("parent", |h| {
                    h.column::<i32>("tree")
                        .column::<Cid>("parent")
                        .column::<Cid>("child")
                })?;

                p.output("root", |h| h.column::<i32>("tree").column::<Cid>("id"))?;

                p.rule::<(i32, Cid, Cid)>("parent", &|h, b, (tree, parent, child)| {
                    h.bind((("tree", tree), ("parent", parent), ("child", child)))?;

                    b.search_cid("evac", parent, (("entity", tree),))?;
                    b.search_cid("evac", child, (("entity", tree),))?;

                    b.search("links", (("from", child), ("to", parent)))?;

                    Ok(())
                })?;

                p.rule::<(i32, Cid)>("root", &|h, b, (tree, root)| {
                    h.bind((("tree", tree), ("id", root)))?;

                    b.search_cid("evac", root, (("entity", tree),))?;
                    b.except("parent", (("child", root), ("tree", tree)))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [f00, f01, f02, f03, f04, f10, f11, f12,],
            idb
        );

        Ok(())
    }

    #[test]
    fn test_get_link_one_hop() -> Result<()> {
        let f0 = InputTuple::new(0, "node", 0, []);
        let f1 = InputTuple::new(0, "node", 0, [f0.cid()?]);
        let f2 = InputTuple::new(0, "node", 0, [f1.cid()?]);
        let f3 = InputTuple::new(0, "node", 0, [f1.cid()?]);
        let f4 = InputTuple::new(0, "node", 0, [f2.cid()?]);
        let f5 = InputTuple::new(0, "node", 0, [f3.cid()?]);
        let f6 = InputTuple::new(0, "node", 0, [f4.cid()?]);

        let idb = [(
            "hop",
            vec![
                Tuple::new(
                    "hop",
                    [("from", Val::Cid(f2.cid()?)), ("to", Val::Cid(f0.cid()?))],
                    None,
                ),
                Tuple::new(
                    "hop",
                    [("from", Val::Cid(f3.cid()?)), ("to", Val::Cid(f0.cid()?))],
                    None,
                ),
                Tuple::new(
                    "hop",
                    [("from", Val::Cid(f4.cid()?)), ("to", Val::Cid(f1.cid()?))],
                    None,
                ),
                Tuple::new(
                    "hop",
                    [("from", Val::Cid(f5.cid()?)), ("to", Val::Cid(f1.cid()?))],
                    None,
                ),
                Tuple::new(
                    "hop",
                    [("from", Val::Cid(f6.cid()?)), ("to", Val::Cid(f2.cid()?))],
                    None,
                ),
            ],
        )];

        assert_derives!(
            |p| {
                p.output("hop", |h| h.column::<Cid>("from").column::<Cid>("to"))?;

                p.rule::<(Cid, Cid, Cid)>("hop", &|h, b, (from, via, to)| {
                    h.bind((("from", from), ("to", to)))?;

                    b.search_cid("evac", from, ())?;
                    b.search("links", (("from", from), ("to", via)))?;
                    b.search("links", (("from", via), ("to", to)))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [f0, f1, f2, f3, f4, f5, f6],
            idb
        );

        Ok(())
    }

    #[test]
    fn test_user_defined_predicate() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("triangle", |h| {
                    h.column::<i32>("a").column::<i32>("b").column::<i32>("c")
                })?;

                p.rule::<(i32, i32, i32)>("triangle", &|h, b, (x, y, z)| {
                    h.bind((("a", x), ("b", y), ("c", z)))?;

                    b.search("evac", (("value", x),))?;
                    b.search("evac", (("value", y),))?;
                    b.search("evac", (("value", z),))?;

                    b.predicate(is_triangle(x, y, z))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [(
                "triangle",
                [
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 3)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 2), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 2), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 3), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 1), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 1), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 2), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 3), ("b", 1), ("c", 5)], None),
                ]
            )]
        );

        Ok(())
    }

    #[test]
    fn test_user_defined_fun_predicate() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("triangle", |h| {
                    h.column::<i32>("a").column::<i32>("b").column::<i32>("c")
                })?;

                p.rule::<(i32, i32, i32)>("triangle", &|h, b, (x, y, z)| {
                    h.bind((("a", x), ("b", y), ("c", z)))?;

                    b.search("evac", (("value", x),))?;
                    b.search("evac", (("value", y),))?;
                    b.search("evac", (("value", z),))?;

                    b.predicate(kernel::when((x, y, z), |(x, y, z)| x + y < z))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [(
                "triangle",
                [
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 3)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 1), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 2), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 2), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 1), ("b", 3), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 1), ("c", 4)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 1), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 2), ("b", 2), ("c", 5)], None),
                    Tuple::new("triangle", [("a", 3), ("b", 1), ("c", 5)], None),
                ]
            )]
        );

        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("count", |h| h.column::<i32>("n"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    h.bind((("n", x),))?;
                    b.search("evac", (("value", x),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32)>("count", &|h, b, (count, n)| {
                    h.bind((("n", count),))?;
                    b.group_by(count, "num", (("n", n),), math::count())?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [("count", [Tuple::new("count", [("n", 5),], None),]),]
        );

        Ok(())
    }

    #[test]
    fn test_sum() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("sum", |h| h.column::<i32>("n"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    h.bind((("n", x),))?;
                    b.search("evac", (("value", x),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32)>("sum", &|h, b, (sum, n)| {
                    h.bind((("n", sum),))?;
                    b.group_by(sum, "num", (("n", n),), math::sum(n))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [("sum", [Tuple::new("sum", [("n", 15),], None),]),]
        );

        Ok(())
    }

    #[test]
    fn test_min() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("min", |h| h.column::<i32>("n"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    h.bind((("n", x),))?;
                    b.search("evac", (("value", x),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32)>("min", &|h, b, (min, n)| {
                    h.bind((("n", min),))?;
                    b.group_by(min, "num", (("n", n),), math::min(n))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [("min", [Tuple::new("min", [("n", 1),], None),]),]
        );

        Ok(())
    }

    #[test]
    fn test_max() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("max", |h| h.column::<i32>("n"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    h.bind((("n", x),))?;
                    b.search("evac", (("value", x),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32)>("max", &|h, b, (max, n)| {
                    h.bind((("n", max),))?;
                    b.group_by(max, "num", (("n", n),), math::max(n))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [("max", [Tuple::new("max", [("n", 5),], None),]),]
        );

        Ok(())
    }

    #[test]
    fn test_mean() -> Result<()> {
        assert_derives!(
            |p| {
                p.output("num", |h| h.column::<i32>("n"))?;
                p.output("mean", |h| h.column::<i32>("n"))?;

                p.rule::<(i32,)>("num", &|h, b, (x,)| {
                    h.bind((("n", x),))?;
                    b.search("evac", (("value", x),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32)>("mean", &|h, b, (mean, n)| {
                    h.bind((("n", mean),))?;
                    b.group_by(mean, "num", (("n", n),), math::mean(n))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [
                InputTuple::new(0, "n", 1, []),
                InputTuple::new(0, "n", 2, []),
                InputTuple::new(0, "n", 3, []),
                InputTuple::new(0, "n", 4, []),
                InputTuple::new(0, "n", 5, []),
            ],
            [("mean", [Tuple::new("mean", [("n", 3),], None),]),]
        );

        Ok(())
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
                    h.bind((("x", x), ("y", y)))?;

                    b.search("num", (("n", x),))?;
                    b.search("num", (("n", y),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32, i32)>("product", &|h, b, (x, y, z)| {
                    h.bind((("z", z),))?;

                    b.group_by(z, "pair", (("x", x), ("y", y)), product(x, y))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [("product", [Tuple::new("product", [("z", 225),], None),]),]
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
                    h.bind((("x", x), ("y", y)))?;

                    b.search("num", (("n", x),))?;
                    b.search("num", (("n", y),))?;

                    Ok(())
                })?;

                p.rule::<(i32, i32, i32)>("product", &|h, b, (x, y, z)| {
                    h.bind((("x", x), ("y", y), ("z", z)))?;

                    b.search("pair", (("x", x), ("y", y)))?;
                    b.group_by(z, "pair", (("x", x), ("y", y)), product(x, y))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [(
                "product",
                [
                    Tuple::new("product", [("x", 1), ("y", 1), ("z", 1),], None),
                    Tuple::new("product", [("x", 1), ("y", 2), ("z", 2),], None),
                    Tuple::new("product", [("x", 1), ("y", 3), ("z", 3),], None),
                    Tuple::new("product", [("x", 1), ("y", 4), ("z", 4),], None),
                    Tuple::new("product", [("x", 1), ("y", 5), ("z", 5),], None),
                    Tuple::new("product", [("x", 2), ("y", 1), ("z", 2),], None),
                    Tuple::new("product", [("x", 2), ("y", 2), ("z", 4),], None),
                    Tuple::new("product", [("x", 2), ("y", 3), ("z", 6),], None),
                    Tuple::new("product", [("x", 2), ("y", 4), ("z", 8),], None),
                    Tuple::new("product", [("x", 2), ("y", 5), ("z", 10),], None),
                    Tuple::new("product", [("x", 3), ("y", 1), ("z", 3),], None),
                    Tuple::new("product", [("x", 3), ("y", 2), ("z", 6),], None),
                    Tuple::new("product", [("x", 3), ("y", 3), ("z", 9),], None),
                    Tuple::new("product", [("x", 3), ("y", 4), ("z", 12),], None),
                    Tuple::new("product", [("x", 3), ("y", 5), ("z", 15),], None),
                    Tuple::new("product", [("x", 4), ("y", 1), ("z", 4),], None),
                    Tuple::new("product", [("x", 4), ("y", 2), ("z", 8),], None),
                    Tuple::new("product", [("x", 4), ("y", 3), ("z", 12),], None),
                    Tuple::new("product", [("x", 4), ("y", 4), ("z", 16),], None),
                    Tuple::new("product", [("x", 4), ("y", 5), ("z", 20),], None),
                    Tuple::new("product", [("x", 5), ("y", 1), ("z", 5),], None),
                    Tuple::new("product", [("x", 5), ("y", 2), ("z", 10),], None),
                    Tuple::new("product", [("x", 5), ("y", 3), ("z", 15),], None),
                    Tuple::new("product", [("x", 5), ("y", 4), ("z", 20),], None),
                    Tuple::new("product", [("x", 5), ("y", 5), ("z", 25),], None),
                ]
            ),]
        );
    }

    #[test]
    fn test_self_join_str() {
        assert_derives!(
            |p| {
                p.output("pair1", |h| {
                    h.column::<&str>("id")
                        .column::<&str>("x")
                        .column::<&str>("y")
                })?;
                p.output("pair2", |h| {
                    h.column::<&str>("id")
                        .column::<&str>("x")
                        .column::<&str>("y")
                })?;

                p.fact("pair1", |f| f.bind((("id", "a"), ("x", "1"), ("y", "2"))))?;
                p.fact("pair1", |f| f.bind((("id", "b"), ("x", "3"), ("y", "4"))))?;
                p.fact("pair1", |f| f.bind((("id", "a"), ("x", "5"), ("y", "6"))))?;

                p.rule::<(&str, &str, &str)>("pair2", &|h, b, (id, x, y)| {
                    h.bind((("id", id), ("x", x), ("y", y)))?;
                    b.search("pair1", (("id", id), ("x", x)))?;
                    b.search("pair1", (("id", id), ("y", y)))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [(
                "pair2",
                [
                    Tuple::new("pair2", [("id", "a"), ("x", "1"), ("y", "2")], None),
                    Tuple::new("pair2", [("id", "a"), ("x", "1"), ("y", "6")], None),
                    Tuple::new("pair2", [("id", "a"), ("x", "5"), ("y", "2")], None),
                    Tuple::new("pair2", [("id", "a"), ("x", "5"), ("y", "6")], None),
                    Tuple::new("pair2", [("id", "b"), ("x", "3"), ("y", "4")], None),
                ]
            )]
        );
    }

    #[test]
    fn test_group_by_any() {
        assert_derives!(
            |p| {
                p.output("p", |h| h.column::<Any>("x"))?;
                p.output("min", |h| h.column::<Any>("x"))?;

                p.fact("p", |f| f.bind((("x", 5),)))?;
                p.fact("p", |f| f.bind((("x", 12),)))?;
                p.fact("p", |f| f.bind((("x", -7),)))?;

                p.rule::<Any>("min", &|h, b, x| {
                    h.bind((("x", x),))?;
                    b.group_by(x, "p", (("x", x),), math::min(x))?;

                    Ok(())
                })?;

                Ok(p)
            },
            [("min", [Tuple::new("min", [("x", -7),], None),])]
        );
    }

    #[derive(Debug)]
    #[allow(unreachable_pub)]
    pub struct Product<T: RhizomeType + AddAssign + WrappingMul + Zero>(T);

    impl<T: RhizomeType + AddAssign + WrappingMul + Zero> Default for Product<T> {
        fn default() -> Self {
            Self(Zero::zero())
        }
    }

    impl<T: RhizomeType + AddAssign + WrappingMul + Zero> Aggregate for Product<T> {
        type Input = (T, T);
        type Output = T;

        fn step(&mut self, (a, b): (T, T)) {
            self.0 += a * b;
        }

        fn finalize(&self) -> Option<Self::Output> {
            Some(self.0.clone())
        }
    }

    rhizome_fn! {
        #[aggregate = Product]
        fn product<T: RhizomeType + AddAssign + WrappingMul + Zero>(a: T, b: T) -> T;
    }

    #[derive(Debug)]
    #[allow(unreachable_pub)]
    pub struct IsTriangle<T>(PhantomData<T>);

    impl<T> Default for IsTriangle<T> {
        fn default() -> Self {
            Self(Default::default())
        }
    }

    impl<T: RhizomeType + Add<Output = T> + Ord> Predicate for IsTriangle<(T, T, T)> {
        type Input = (T, T, T);

        fn apply(&self, (a, b, c): Self::Input) -> Option<bool> {
            Some(a + b < c)
        }
    }

    rhizome_fn! {
        #[predicate = IsTriangle]
        fn is_triangle<T: RhizomeType + Add<Output = T> + Ord>(a: T, b: T, z: T) -> T;
    }
}

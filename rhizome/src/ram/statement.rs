use derive_more::IsVariant;
use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    pretty::Pretty,
    relation::Relation,
};

pub(crate) mod exit;
pub(crate) mod insert;
pub(crate) mod merge;
pub(crate) mod purge;
pub(crate) mod recursive;
pub(crate) mod sinks;
pub(crate) mod sources;
pub(crate) mod swap;

pub(crate) use exit::*;
pub(crate) use insert::*;
pub(crate) use merge::*;
pub(crate) use purge::*;
pub(crate) use recursive::*;
pub(crate) use sinks::*;
pub(crate) use sources::*;
pub(crate) use swap::*;

// TODO: Flatten the AST by replacing `Loop` with `Push` and `Pop`;
// see https://github.com/RhizomeDB/rs-rhizome/issues/29
#[derive(Debug, IsVariant)]
pub(crate) enum Statement<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Insert(Insert<EF, IF, ER, IR>),
    Merge(Merge<EF, IF, ER, IR>),
    Swap(Swap<IR>),
    Purge(Purge<EF, IF, ER, IR>),
    Loop(Loop<EF, IF, ER, IR>),
    Exit(Exit<IF, IR>),
    Sources(Sources<EF, ER>),
    Sinks(Sinks<IF, IR>),
}

impl<EF, IF, ER, IR> Pretty for Statement<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Statement::Insert(inner) => inner.to_doc(),
            Statement::Merge(inner) => inner.to_doc(),
            Statement::Swap(inner) => inner.to_doc(),
            Statement::Purge(inner) => inner.to_doc(),
            Statement::Loop(inner) => inner.to_doc(),
            Statement::Exit(inner) => inner.to_doc(),
            Statement::Sources(inner) => inner.to_doc(),
            Statement::Sinks(inner) => inner.to_doc(),
        }
    }
}

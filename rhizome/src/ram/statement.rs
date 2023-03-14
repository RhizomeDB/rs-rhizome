use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

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

// TODO: Nested loops shouldn't be supported, so I should split the AST
// to make them unrepresentable.
#[derive(Clone, Debug, IsVariant)]
pub enum Statement {
    Insert(Insert),
    Merge(Merge),
    Swap(Swap),
    Purge(Purge),
    Loop(Loop),
    Exit(Exit),
    Sources(Sources),
    Sinks(Sinks),
}

impl Pretty for Statement {
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

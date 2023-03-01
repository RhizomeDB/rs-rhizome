use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

pub mod exit;
pub mod insert;
pub mod merge;
pub mod purge;
pub mod recursive;
pub mod sinks;
pub mod sources;
pub mod swap;

pub use exit::*;
pub use insert::*;
pub use merge::*;
pub use purge::*;
pub use recursive::*;
pub use sinks::*;
pub use sources::*;
pub use swap::*;

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

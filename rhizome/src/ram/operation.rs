use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

pub(crate) mod project;
pub(crate) mod reduce;
pub(crate) mod search;

pub(crate) use project::*;
pub(crate) use reduce::*;
pub(crate) use search::*;

#[derive(Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
    Reduce(Reduce),
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
            Operation::Reduce(inner) => inner.to_doc(),
        }
    }
}

use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

pub(crate) mod aggregate;
pub(crate) mod project;
pub(crate) mod search;

pub(crate) use aggregate::*;
pub(crate) use project::*;
pub(crate) use search::*;

#[derive(Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
    Aggregate(Aggregate),
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
            Operation::Aggregate(inner) => inner.to_doc(),
        }
    }
}

use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

pub(crate) mod aggregation;
pub(crate) mod project;
pub(crate) mod search;

pub(crate) use aggregation::*;
pub(crate) use project::*;
pub(crate) use search::*;

#[derive(Debug, IsVariant)]
pub(crate) enum Operation {
    Search(Search),
    Project(Project),
    Aggregation(Aggregation),
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
            Operation::Aggregation(inner) => inner.to_doc(),
        }
    }
}

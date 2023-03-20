use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

// pub mod get_link;
pub(crate) mod project;
pub(crate) mod search;

// pub use get_link::*;
pub(crate) use project::*;
pub(crate) use search::*;

#[derive(Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
        }
    }
}

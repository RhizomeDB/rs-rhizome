use derive_more::IsVariant;
use pretty::RcDoc;

use crate::pretty::Pretty;

pub mod get_link;
pub mod project;
pub mod search;

pub use get_link::*;
pub use project::*;
pub use search::*;

#[derive(Clone, Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
    GetLink(GetLink),
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
            Operation::GetLink(inner) => inner.to_doc(),
        }
    }
}

use derive_more::IsVariant;
use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    pretty::Pretty,
    relation::Relation,
};

pub(crate) mod project;
pub(crate) mod reduce;
pub(crate) mod search;

pub(crate) use project::*;
pub(crate) use reduce::*;
pub(crate) use search::*;

#[derive(Debug, IsVariant)]
pub(crate) enum Operation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Search(Search<EF, IF, ER, IR>),
    Project(Project<EF, IF, ER, IR>),
    Reduce(Reduce<EF, IF, ER, IR>),
}

impl<EF, IF, ER, IR> Pretty for Operation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search(inner) => inner.to_doc(),
            Operation::Project(inner) => inner.to_doc(),
            Operation::Reduce(inner) => inner.to_doc(),
        }
    }
}

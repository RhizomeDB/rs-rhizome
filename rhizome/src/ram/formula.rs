use std::sync::Arc;

use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    predicate::PredicateWrapper,
    pretty::Pretty,
    relation::Relation,
};

use super::{predicate::Predicate, Equality, NotIn, NotInRelation, RelationVersion, Term};

#[derive(Debug, Clone, IsVariant, From, TryInto)]
pub(crate) enum Formula<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Equality(Equality),
    NotIn(NotIn<EF, IF, ER, IR>),
    Predicate(Predicate),
}

impl<EF, IF, ER, IR> Formula<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn equality(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        Self::Equality(Equality::new(left, right))
    }

    pub(crate) fn not_in<A, T>(
        id: RelationId,
        version: RelationVersion,
        cols: impl IntoIterator<Item = (A, T)>,
        relation: NotInRelation<EF, IF, ER, IR>,
    ) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        Self::NotIn(NotIn::new(id, version, cols, relation))
    }

    pub(crate) fn predicate(terms: Vec<Term>, f: Arc<dyn PredicateWrapper>) -> Self {
        Self::Predicate(Predicate::new(terms, f))
    }
}

impl<EF, IF, ER, IR> Pretty for Formula<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Formula::Equality(inner) => inner.to_doc(),
            Formula::NotIn(inner) => inner.to_doc(),
            Formula::Predicate(inner) => inner.to_doc(),
        }
    }
}

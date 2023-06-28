use std::sync::{Arc, RwLock};

use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColId, RelationId},
    predicate::PredicateWrapper,
    pretty::Pretty,
    relation::{Relation, Version},
};

use super::{predicate::Predicate, Equality, NotIn, Term};

#[derive(Debug, Clone, IsVariant, From, TryInto)]
pub(crate) enum Formula {
    Equality(Equality),
    NotIn(NotIn),
    Predicate(Predicate),
}

impl Formula {
    pub(crate) fn equality(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        Self::Equality(Equality::new(left, right))
    }

    pub(crate) fn not_in<A, T>(
        id: RelationId,
        version: Version,
        cols: impl IntoIterator<Item = (A, T)>,
        relation: Arc<RwLock<Box<dyn Relation>>>,
    ) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        Self::NotIn(NotIn::new((id, version), cols, relation))
    }

    pub(crate) fn predicate(terms: Vec<Term>, f: Arc<dyn PredicateWrapper>) -> Self {
        Self::Predicate(Predicate::new(terms, f))
    }
}

impl Pretty for Formula {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Formula::Equality(inner) => inner.to_doc(),
            Formula::NotIn(inner) => inner.to_doc(),
            Formula::Predicate(inner) => inner.to_doc(),
        }
    }
}

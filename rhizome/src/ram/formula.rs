use std::sync::Arc;

use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{id::ColId, logic::VarClosure, pretty::Pretty};

use super::{predicate::Predicate, Equality, NotIn, RelationRef, Term};

#[derive(Debug, IsVariant, From, TryInto)]
pub enum Formula {
    Equality(Equality),
    NotIn(NotIn),
    Predicate(Predicate),
}

impl Formula {
    pub fn equality(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        Self::Equality(Equality::new(left, right))
    }

    pub fn not_in<A, T>(cols: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        Self::NotIn(NotIn::new(cols, relation))
    }

    pub fn predicate(terms: Vec<Term>, f: Arc<dyn VarClosure>) -> Self {
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

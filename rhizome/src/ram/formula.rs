use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{id::ColumnId, pretty::Pretty};

use super::{Equality, NotIn, RelationRef, Term};

#[derive(Clone, Debug, IsVariant, From, TryInto)]
pub enum Formula {
    Equality(Equality),
    NotIn(NotIn),
}

impl Formula {
    pub fn equality(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        Self::Equality(Equality::new(left, right))
    }

    pub fn not_in<A, T>(attributes: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<ColumnId>,
        T: Into<Term>,
    {
        Self::NotIn(NotIn::new(attributes, relation))
    }
}

impl Pretty for Formula {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Formula::Equality(inner) => inner.to_doc(),
            Formula::NotIn(inner) => inner.to_doc(),
        }
    }
}

use derive_more::{From, IsVariant, TryInto};

use crate::id::RelationId;

use super::{Edge, Fact, Rule};

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum Clause {
    Fact(Fact),
    Rule(Rule),
}

impl Clause {
    pub fn head(&self) -> RelationId {
        match self {
            Clause::Fact(fact) => fact.head(),
            Clause::Rule(rule) => rule.head(),
        }
    }

    pub fn depends_on(&self) -> Vec<Edge> {
        match self {
            Self::Fact(fact) => fact.depends_on(),
            Self::Rule(rule) => rule.depends_on(),
        }
    }
}

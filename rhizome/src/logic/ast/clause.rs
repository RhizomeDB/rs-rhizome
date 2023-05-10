use crate::id::RelationId;

use super::{Fact, Rule};

#[derive(Debug)]
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
}

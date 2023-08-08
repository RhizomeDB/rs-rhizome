use std::collections::HashSet;

use crate::id::RelationId;

use super::{Clause, Fact, Rule};

#[derive(Debug)]
pub(crate) struct Stratum<'a> {
    relations: HashSet<RelationId>,
    clauses: Vec<&'a Clause>,
    is_recursive: bool,
}

impl<'a> Stratum<'a> {
    pub(crate) fn new(
        relations: HashSet<RelationId>,
        clauses: Vec<&'a Clause>,
        is_recursive: bool,
    ) -> Self {
        Self {
            relations,
            clauses,
            is_recursive,
        }
    }

    pub(crate) fn relations(&self) -> &HashSet<RelationId> {
        &self.relations
    }

    pub(crate) fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    pub(crate) fn facts(&self) -> Vec<&Fact> {
        self.clauses
            .iter()
            .filter_map(|term| {
                if let Clause::Fact(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn rules(&self) -> Vec<&Rule> {
        self.clauses
            .iter()
            .filter_map(|term| {
                if let Clause::Rule(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }
}

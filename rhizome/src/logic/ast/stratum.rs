use std::collections::HashSet;

use crate::id::RelationId;

use super::{Clause, Fact, Rule};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct Stratum {
    relations: HashSet<RelationId>,
    clauses: Vec<Clause>,
    is_recursive: bool,
}

impl Stratum {
    pub(crate) fn new(
        relations: HashSet<RelationId>,
        clauses: Vec<Clause>,
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

    pub(crate) fn facts(&self) -> Vec<Fact> {
        self.clauses_of::<Fact>()
    }

    pub(crate) fn rules(&self) -> Vec<Rule> {
        self.clauses_of::<Rule>()
    }

    fn clauses_of<T>(&self) -> Vec<T>
    where
        T: TryFrom<Clause>,
    {
        self.clauses
            .iter()
            .filter_map(|clause| T::try_from(clause.clone()).ok())
            .collect()
    }
}

use std::sync::Arc;

use super::{Clause, Declaration};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Program {
    declarations: Vec<Arc<Declaration>>,
    clauses: Vec<Clause>,
}

impl Program {
    pub fn new(declarations: Vec<Arc<Declaration>>, clauses: Vec<Clause>) -> Self {
        Self {
            declarations,
            clauses,
        }
    }

    pub fn declarations(&self) -> &Vec<Arc<Declaration>> {
        &self.declarations
    }

    pub fn clauses(&self) -> &Vec<Clause> {
        &self.clauses
    }
}

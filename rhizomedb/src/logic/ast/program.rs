use std::sync::Arc;

use super::{Clause, Declaration};

#[derive(Debug)]
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

    pub fn declarations(&self) -> &[Arc<Declaration>] {
        &self.declarations
    }

    pub fn clauses(&self) -> &[Clause] {
        &self.clauses
    }
}

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    pretty::Pretty,
    relation::{Relation, RelationKey},
};

use super::Statement;

#[derive(Debug)]
pub struct Program {
    is_monotonic: bool,
    relations: HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
    statements: Vec<Arc<Statement>>,
}

impl Program {
    pub(crate) fn new(
        is_monotonic: bool,
        relations: HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
        statements: Vec<Arc<Statement>>,
    ) -> Self {
        Self {
            is_monotonic,
            relations,
            statements,
        }
    }

    pub(crate) fn is_monotonic(&self) -> bool {
        self.is_monotonic
    }

    pub(crate) fn relations(&self) -> &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>> {
        &self.relations
    }

    pub(crate) fn statements(&self) -> &[Arc<Statement>] {
        &self.statements
    }
}

impl Pretty for Program {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::intersperse(
            self.statements().iter().map(|statement| statement.to_doc()),
            RcDoc::text(";")
                .append(RcDoc::hardline())
                .append(RcDoc::hardline()),
        )
        .append(RcDoc::text(";"))
    }
}

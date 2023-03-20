use std::sync::Arc;

use pretty::RcDoc;

use crate::{pretty::Pretty, schema::Schema};

use super::Statement;

#[derive(Debug)]
pub struct Program {
    inputs: Vec<Arc<Schema>>,
    outputs: Vec<Arc<Schema>>,
    statements: Vec<Arc<Statement>>,
}

impl Program {
    pub fn new(
        inputs: Vec<Arc<Schema>>,
        outputs: Vec<Arc<Schema>>,
        statements: Vec<Arc<Statement>>,
    ) -> Self {
        Self {
            inputs,
            outputs,
            statements,
        }
    }

    pub fn inputs(&self) -> &[Arc<Schema>] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[Arc<Schema>] {
        &self.outputs
    }

    pub fn statements(&self) -> &[Arc<Statement>] {
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

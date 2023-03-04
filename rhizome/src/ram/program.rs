use std::sync::Arc;

use pretty::RcDoc;

use crate::{
    logic::ast::declaration::InnerDeclaration,
    pretty::Pretty,
    relation::{EDB, IDB},
};

use super::Statement;

#[derive(Clone, Debug)]
pub struct Program {
    inputs: Vec<Arc<InnerDeclaration<EDB>>>,
    outputs: Vec<Arc<InnerDeclaration<IDB>>>,
    statements: Vec<Statement>,
}

impl Program {
    pub fn new(
        inputs: Vec<Arc<InnerDeclaration<EDB>>>,
        outputs: Vec<Arc<InnerDeclaration<IDB>>>,
        statements: Vec<Statement>,
    ) -> Self {
        Self {
            inputs,
            outputs,
            statements,
        }
    }

    pub fn inputs(&self) -> &Vec<Arc<InnerDeclaration<EDB>>> {
        &self.inputs
    }

    pub fn outputs(&self) -> &Vec<Arc<InnerDeclaration<IDB>>> {
        &self.outputs
    }

    pub fn statements(&self) -> &Vec<Statement> {
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

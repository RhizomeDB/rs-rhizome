use std::sync::Arc;

use pretty::RcDoc;

use crate::pretty::Pretty;

use super::Statement;

#[derive(Debug)]
pub struct Program {
    statements: Vec<Arc<Statement>>,
}

impl Program {
    pub(crate) fn new(statements: Vec<Arc<Statement>>) -> Self {
        Self { statements }
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

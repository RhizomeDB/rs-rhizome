use std::sync::Arc;

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    pretty::Pretty,
    relation::Relation,
};

use super::Statement;

#[derive(Debug)]
pub struct Program<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    statements: Vec<Arc<Statement<EF, IF, ER, IR>>>,
}

impl<EF, IF, ER, IR> Program<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new(statements: Vec<Arc<Statement<EF, IF, ER, IR>>>) -> Self {
        Self { statements }
    }

    pub(crate) fn statements(&self) -> &[Arc<Statement<EF, IF, ER, IR>>] {
        &self.statements
    }
}

impl<EF, IF, ER, IR> Pretty for Program<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
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

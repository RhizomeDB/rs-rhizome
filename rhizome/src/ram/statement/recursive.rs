use std::sync::Arc;

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    pretty::Pretty,
    relation::Relation,
};

use super::Statement;

#[derive(Debug)]
pub(crate) struct Loop<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    body: Vec<Arc<Statement<EF, IF, ER, IR>>>,
}

impl<EF, IF, ER, IR> Loop<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new(body: impl IntoIterator<Item = Arc<Statement<EF, IF, ER, IR>>>) -> Self {
        let body = body.into_iter().collect();

        Self { body }
    }

    pub(crate) fn body(&self) -> &[Arc<Statement<EF, IF, ER, IR>>] {
        &self.body
    }
}

impl<EF, IF, ER, IR> Pretty for Loop<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let body_doc = RcDoc::hardline()
            .append(RcDoc::intersperse(
                self.body().iter().map(|statement| statement.to_doc()),
                RcDoc::text(";")
                    .append(RcDoc::hardline())
                    .append(RcDoc::hardline()),
            ))
            .nest(2)
            .group();

        RcDoc::text("loop do")
            .append(body_doc)
            .append(RcDoc::text(";"))
            .append(RcDoc::hardline())
            .append(RcDoc::text("end"))
    }
}

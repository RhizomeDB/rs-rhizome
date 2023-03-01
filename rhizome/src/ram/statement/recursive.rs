use pretty::RcDoc;

use crate::pretty::Pretty;

use super::Statement;

#[derive(Clone, Debug)]
pub struct Loop {
    body: Vec<Statement>,
}

impl Loop {
    pub fn new(body: impl IntoIterator<Item = Statement>) -> Self {
        let body = body.into_iter().collect();

        Self { body }
    }

    pub fn body(&self) -> &Vec<Statement> {
        &self.body
    }
}

impl Pretty for Loop {
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

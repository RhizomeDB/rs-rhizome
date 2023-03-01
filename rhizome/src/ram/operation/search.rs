use pretty::RcDoc;

use crate::{
    pretty::Pretty,
    ram::{alias_id::AliasId, formula::Formula, relation_ref::RelationRef},
};

use super::Operation;

#[derive(Clone, Debug)]
pub struct Search {
    relation: RelationRef,
    alias: Option<AliasId>,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl Search {
    pub fn new(
        relation: RelationRef,
        alias: Option<AliasId>,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            relation,
            alias,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }

    pub fn alias(&self) -> &Option<AliasId> {
        &self.alias
    }

    pub fn when(&self) -> &Vec<Formula> {
        &self.when
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

impl Pretty for Search {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relation_doc = match self.alias() {
            Some(alias) => RcDoc::concat([
                self.relation().to_doc(),
                RcDoc::text(" as "),
                self.relation().to_doc(),
                RcDoc::text("_"),
                RcDoc::as_string(alias),
            ]),
            None => self.relation().to_doc(),
        };

        let when_doc = if self.when().is_empty() {
            RcDoc::nil()
        } else {
            RcDoc::text(" where")
                .append(RcDoc::hardline())
                .append(RcDoc::text("("))
                .append(
                    RcDoc::intersperse(
                        self.when().iter().map(|formula| formula.to_doc()),
                        RcDoc::text(" and "),
                    )
                    .nest(1)
                    .group(),
                )
                .append(RcDoc::text(")"))
        };

        RcDoc::concat([
            RcDoc::text("search "),
            relation_doc,
            when_doc,
            RcDoc::text(" do"),
        ])
        .append(
            RcDoc::hardline()
                .append(self.operation().to_doc())
                .nest(2)
                .group(),
        )
    }
}

use pretty::RcDoc;

use crate::{pretty::Pretty, ram::relation_ref::RelationRef};

#[derive(Clone, Debug)]
pub struct Exit {
    relations: Vec<RelationRef>,
}

impl Exit {
    pub fn new(relations: impl IntoIterator<Item = RelationRef>) -> Self {
        let relations = relations.into_iter().collect();

        Self { relations }
    }

    pub fn relations(&self) -> &[RelationRef] {
        &self.relations
    }
}

impl Pretty for Exit {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations().iter().map(|r| {
                RcDoc::text("count(")
                    .append(r.to_doc())
                    .append(RcDoc::text(") == 0"))
            }),
            RcDoc::text(" or "),
        )
        .nest(1)
        .group();

        RcDoc::text("exit if ").append(relations_doc)
    }
}

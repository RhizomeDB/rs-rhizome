use im::ImHashSet;
use pretty::RcDoc;

use crate::{pretty::Pretty, ram::relation_ref::RelationRef};

#[derive(Clone, Debug)]
pub struct Sinks {
    relations: im::ImHashSet<RelationRef>,
}

impl Sinks {
    pub fn new(relations: impl IntoIterator<Item = RelationRef>) -> Self {
        let relations = relations.into_iter().collect();

        Self { relations }
    }

    pub fn relations(&self) -> &ImHashSet<RelationRef> {
        &self.relations
    }
}

impl Pretty for Sinks {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations().iter().map(|r| r.to_doc()),
            RcDoc::text(", "),
        );

        RcDoc::text("sinks ").append(relations_doc)
    }
}

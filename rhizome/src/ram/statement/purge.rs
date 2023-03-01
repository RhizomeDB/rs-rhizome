use pretty::RcDoc;

use crate::{pretty::Pretty, ram::relation_ref::RelationRef};

#[derive(Clone, Copy, Debug)]
pub struct Purge {
    relation: RelationRef,
}

impl Purge {
    pub fn new(relation: RelationRef) -> Self {
        Self { relation }
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }
}

impl Pretty for Purge {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::text("purge ").append(self.relation().to_doc())
    }
}

use pretty::RcDoc;

use crate::{pretty::Pretty, ram::relation_ref::RelationRef};

#[derive(Clone, Copy, Debug)]
pub struct Merge {
    from: RelationRef,
    into: RelationRef,
}

impl Merge {
    pub fn new(from: RelationRef, into: RelationRef) -> Self {
        Self { from, into }
    }

    pub fn from(&self) -> &RelationRef {
        &self.from
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

impl Pretty for Merge {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::text("merge ")
            .append(self.from().to_doc())
            .append(RcDoc::text(" into "))
            .append(self.into().to_doc())
    }
}

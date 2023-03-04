use pretty::RcDoc;

use crate::{pretty::Pretty, ram::relation_ref::RelationRef};

#[derive(Clone, Copy, Debug)]
pub struct Swap {
    left: RelationRef,
    right: RelationRef,
}

impl Swap {
    pub fn new(left: RelationRef, right: RelationRef) -> Self {
        Self { left, right }
    }

    pub fn left(&self) -> &RelationRef {
        &self.left
    }

    pub fn right(&self) -> &RelationRef {
        &self.right
    }
}

impl Pretty for Swap {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::text("swap ")
            .append(self.left().to_doc())
            .append(RcDoc::text(" and "))
            .append(self.right().to_doc())
    }
}

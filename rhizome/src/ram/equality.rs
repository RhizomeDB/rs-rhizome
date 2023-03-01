use pretty::RcDoc;

use crate::pretty::Pretty;

use super::Term;

#[derive(Clone, Debug)]
pub struct Equality {
    left: Term,
    right: Term,
}

impl Equality {
    pub fn new(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        let left = left.into();
        let right = right.into();

        Self { left, right }
    }

    pub fn left(&self) -> &Term {
        &self.left
    }

    pub fn right(&self) -> &Term {
        &self.right
    }
}

impl Pretty for Equality {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            self.left().to_doc(),
            RcDoc::text(" = "),
            self.right().to_doc(),
        ])
        .group()
    }
}

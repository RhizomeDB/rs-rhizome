use pretty::RcDoc;

use crate::pretty::Pretty;

use super::Term;

#[derive(Clone, Debug)]
pub(crate) struct Equality {
    left: Term,
    right: Term,
}

impl Equality {
    pub(crate) fn new(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        let left = left.into();
        let right = right.into();

        Self { left, right }
    }

    pub(crate) fn left(&self) -> &Term {
        &self.left
    }

    pub(crate) fn right(&self) -> &Term {
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

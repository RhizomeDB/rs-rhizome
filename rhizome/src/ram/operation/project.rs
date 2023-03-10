use std::collections::HashMap;

use pretty::RcDoc;

use crate::{
    id::ColId,
    pretty::Pretty,
    ram::{relation_ref::RelationRef, term::Term},
};

#[derive(Clone, Debug)]
pub struct Project {
    cols: HashMap<ColId, Term>,
    into: RelationRef,
}

impl Project {
    pub fn new<A, T>(cols: impl IntoIterator<Item = (A, T)>, into: RelationRef) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { cols, into }
    }

    pub fn cols(&self) -> &HashMap<ColId, Term> {
        &self.cols
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

impl Pretty for Project {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols().iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(2)
        .group();

        RcDoc::concat([
            RcDoc::text("project "),
            RcDoc::text("("),
            cols_doc,
            RcDoc::text(")"),
            RcDoc::text(" into "),
            self.into().to_doc(),
        ])
    }
}

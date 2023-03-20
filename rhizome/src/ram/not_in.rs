use std::{collections::HashMap};

use pretty::RcDoc;

use crate::{id::ColId, pretty::Pretty};

use super::{RelationRef, Term};

#[derive(Clone, Debug)]
pub struct NotIn {
    cols: HashMap<ColId, Term>,
    relation: RelationRef,
}

impl NotIn {
    pub fn new<A, T>(cols: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { cols, relation }
    }

    pub fn cols(&self) -> &HashMap<ColId, Term> {
        &self.cols
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }
}

impl Pretty for NotIn {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols().iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(1)
        .group();

        RcDoc::concat([
            RcDoc::text("("),
            cols_doc,
            RcDoc::text(")"),
            RcDoc::text(" notin "),
            self.relation().to_doc(),
        ])
    }
}

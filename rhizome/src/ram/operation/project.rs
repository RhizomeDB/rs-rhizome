use std::collections::HashMap;

use pretty::RcDoc;

use crate::{
    id::ColumnId,
    pretty::Pretty,
    ram::{relation_ref::RelationRef, term::Term},
};

#[derive(Clone, Debug)]
pub struct Project {
    attributes: HashMap<ColumnId, Term>,
    into: RelationRef,
}

impl Project {
    pub fn new<A, T>(attributes: impl IntoIterator<Item = (A, T)>, into: RelationRef) -> Self
    where
        A: Into<ColumnId>,
        T: Into<Term>,
    {
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { attributes, into }
    }

    pub fn attributes(&self) -> &HashMap<ColumnId, Term> {
        &self.attributes
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

impl Pretty for Project {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let attributes_doc = RcDoc::intersperse(
            self.attributes().iter().map(|(attribute, term)| {
                RcDoc::concat([
                    RcDoc::as_string(attribute),
                    RcDoc::text(": "),
                    term.to_doc(),
                ])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(2)
        .group();

        RcDoc::concat([
            RcDoc::text("project "),
            RcDoc::text("("),
            attributes_doc,
            RcDoc::text(")"),
            RcDoc::text(" into "),
            self.into().to_doc(),
        ])
    }
}

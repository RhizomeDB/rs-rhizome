use std::collections::HashMap;

use pretty::RcDoc;

use crate::{id::ColumnId, pretty::Pretty};

use super::{RelationRef, Term};

#[derive(Clone, Debug)]
pub struct NotIn {
    attributes: HashMap<ColumnId, Term>,
    relation: RelationRef,
}

impl NotIn {
    pub fn new<A, T>(attributes: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<ColumnId>,
        T: Into<Term>,
    {
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self {
            attributes,
            relation,
        }
    }

    pub fn attributes(&self) -> &HashMap<ColumnId, Term> {
        &self.attributes
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }
}

impl Pretty for NotIn {
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
        .nest(1)
        .group();

        RcDoc::concat([
            RcDoc::text("("),
            attributes_doc,
            RcDoc::text(")"),
            RcDoc::text(" notin "),
            self.relation().to_doc(),
        ])
    }
}

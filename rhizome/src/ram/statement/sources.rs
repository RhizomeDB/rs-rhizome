use std::collections::HashSet;

use pretty::RcDoc;

use crate::{id::RelationId, pretty::Pretty};

#[derive(Clone, Debug)]
pub struct Sources {
    relations: HashSet<RelationId>,
}

impl Sources {
    pub fn relations(&self) -> &HashSet<RelationId> {
        &self.relations
    }
}

impl FromIterator<RelationId> for Sources {
    fn from_iter<T: IntoIterator<Item = RelationId>>(iter: T) -> Self {
        let relations = iter.into_iter().collect();

        Self { relations }
    }
}

impl Pretty for Sources {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations().iter().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sources ").append(relations_doc)
    }
}

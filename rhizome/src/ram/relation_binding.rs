use pretty::RcDoc;

use crate::{id::RelationId, pretty::Pretty, relation::Source};

use super::AliasId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationBinding {
    id: RelationId,
    alias: Option<AliasId>,
    source: Source,
}

impl RelationBinding {
    pub fn new(id: RelationId, alias: Option<AliasId>, source: Source) -> Self {
        Self { id, alias, source }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn alias(&self) -> Option<AliasId> {
        self.alias
    }

    pub fn source(&self) -> Source {
        self.source
    }
}

impl Pretty for RelationBinding {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        if let Some(alias) = self.alias() {
            RcDoc::concat([
                RcDoc::as_string(self.id()),
                RcDoc::text("_"),
                RcDoc::as_string(alias),
            ])
        } else {
            RcDoc::as_string(self.id())
        }
    }
}

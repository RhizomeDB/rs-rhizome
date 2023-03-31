use pretty::RcDoc;

use crate::{id::RelationId, pretty::Pretty, relation::Source};

use super::RelationVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationRef {
    id: RelationId,
    version: RelationVersion,
    source: Source,
}

impl RelationRef {
    pub fn new(id: RelationId, version: RelationVersion, source: Source) -> Self {
        Self {
            id,
            version,
            source,
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn version(&self) -> RelationVersion {
        self.version
    }

    pub fn source(&self) -> Source {
        self.source
    }
}

impl Pretty for RelationRef {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::as_string(self.id()),
            RcDoc::text("_"),
            RcDoc::as_string(self.version()),
        ])
    }
}

use std::marker::PhantomData;

use pretty::RcDoc;

use crate::{
    id::RelationId,
    pretty::Pretty,
    relation::{EDB, IDB},
};

use super::RelationVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationRef {
    EDB(InnerRelationRef<EDB>),
    IDB(InnerRelationRef<IDB>),
}

impl RelationRef {
    pub fn edb(id: RelationId, version: RelationVersion) -> Self {
        Self::EDB(InnerRelationRef::new(id, version))
    }

    pub fn idb(id: RelationId, version: RelationVersion) -> Self {
        Self::IDB(InnerRelationRef::new(id, version))
    }

    pub fn id(&self) -> RelationId {
        match self {
            RelationRef::EDB(inner) => inner.id(),
            RelationRef::IDB(inner) => inner.id(),
        }
    }

    pub fn version(&self) -> RelationVersion {
        match self {
            RelationRef::EDB(inner) => inner.version(),
            RelationRef::IDB(inner) => inner.version(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct InnerRelationRef<T> {
    id: RelationId,
    version: RelationVersion,
    _marker: PhantomData<T>,
}

impl<T> InnerRelationRef<T> {
    pub fn new(id: RelationId, version: RelationVersion) -> Self {
        Self {
            id,
            version,
            _marker: PhantomData::default(),
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn version(&self) -> RelationVersion {
        self.version
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

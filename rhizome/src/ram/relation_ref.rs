use std::marker::PhantomData;

use pretty::RcDoc;

use crate::{
    id::RelationId,
    pretty::Pretty,
    relation::{Edb, Idb},
};

use super::RelationVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationRef {
    Edb(InnerRelationRef<Edb>),
    Idb(InnerRelationRef<Idb>),
}

impl RelationRef {
    pub fn edb(id: RelationId, version: RelationVersion) -> Self {
        Self::Edb(InnerRelationRef::new(id, version))
    }

    pub fn idb(id: RelationId, version: RelationVersion) -> Self {
        Self::Idb(InnerRelationRef::new(id, version))
    }

    pub fn id(&self) -> RelationId {
        match self {
            RelationRef::Edb(inner) => inner.id(),
            RelationRef::Idb(inner) => inner.id(),
        }
    }

    pub fn version(&self) -> RelationVersion {
        match self {
            RelationRef::Edb(inner) => inner.version(),
            RelationRef::Idb(inner) => inner.version(),
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

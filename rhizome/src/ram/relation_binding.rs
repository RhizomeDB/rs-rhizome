use std::marker::PhantomData;

use pretty::RcDoc;

use crate::{
    id::RelationId,
    pretty::Pretty,
    relation::{Edb, Idb},
};

use super::AliasId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationBinding {
    Edb(InnerRelationBinding<Edb>),
    Idb(InnerRelationBinding<Idb>),
}

impl RelationBinding {
    pub fn edb(id: RelationId, alias: Option<AliasId>) -> Self {
        Self::Edb(InnerRelationBinding::new(id, alias))
    }

    pub fn idb(id: RelationId, alias: Option<AliasId>) -> Self {
        Self::Idb(InnerRelationBinding::new(id, alias))
    }

    pub fn id(&self) -> RelationId {
        match self {
            RelationBinding::Edb(inner) => inner.id(),
            RelationBinding::Idb(inner) => inner.id(),
        }
    }

    pub fn alias(&self) -> Option<AliasId> {
        match self {
            RelationBinding::Edb(inner) => inner.alias(),
            RelationBinding::Idb(inner) => inner.alias(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct InnerRelationBinding<T> {
    id: RelationId,
    alias: Option<AliasId>,
    _marker: PhantomData<T>,
}

impl<T> InnerRelationBinding<T> {
    pub fn new(id: RelationId, alias: Option<AliasId>) -> Self {
        Self {
            id,
            alias,
            _marker: PhantomData::default(),
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn alias(&self) -> Option<AliasId> {
        self.alias
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

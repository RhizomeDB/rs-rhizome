use crate::{
    id::RelationId,
    relation::{RelationSource, EDB, IDB},
    schema::Schema,
};
use std::{marker::PhantomData, sync::Arc};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Declaration {
    Edb(InnerDeclaration<EDB>),
    Idb(InnerDeclaration<IDB>),
}

impl Declaration {
    pub fn id(&self) -> RelationId {
        match self {
            Declaration::Edb(inner) => inner.schema().id(),
            Declaration::Idb(inner) => inner.schema().id(),
        }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        match self {
            Declaration::Edb(inner) => inner.schema(),
            Declaration::Idb(inner) => inner.schema(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InnerDeclaration<T> {
    id: RelationId,
    schema: Arc<Schema>,
    _marker: PhantomData<T>,
}

impl<T> InnerDeclaration<T>
where
    T: RelationSource,
{
    pub fn new(id: RelationId, schema: Arc<Schema>) -> Self {
        Self {
            id,
            schema,
            _marker: PhantomData::default(),
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

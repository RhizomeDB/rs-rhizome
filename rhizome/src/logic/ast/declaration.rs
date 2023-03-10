use crate::{
    id::{ColId, RelationId},
    relation::{RelationSource, EDB, IDB},
};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use super::Col;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Declaration {
    EDB(Arc<InnerDeclaration<EDB>>),
    IDB(Arc<InnerDeclaration<IDB>>),
}

impl Declaration {
    pub fn id(&self) -> RelationId {
        match self {
            Declaration::EDB(inner) => inner.id(),
            Declaration::IDB(inner) => inner.id(),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Declaration::EDB(inner) => inner.schema(),
            Declaration::IDB(inner) => inner.schema(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InnerDeclaration<T> {
    id: RelationId,
    schema: Schema,
    _marker: PhantomData<T>,
}

impl<T> InnerDeclaration<T>
where
    T: RelationSource,
{
    pub fn new(id: RelationId, schema: Schema) -> Self {
        Self {
            id,
            schema,
            _marker: PhantomData::default(),
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    cols: HashMap<ColId, Col>,
}

impl Schema {
    pub fn new(cols: HashMap<ColId, Col>) -> Self {
        Self { cols }
    }

    pub fn has_col(&self, k: &ColId) -> bool {
        self.cols().contains_key(k)
    }

    pub fn get_col(&self, k: &ColId) -> Option<&Col> {
        self.cols.get(k)
    }

    pub fn cols(&self) -> &HashMap<ColId, Col> {
        &self.cols
    }
}

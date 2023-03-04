use crate::{
    id::{ColumnId, RelationId},
    relation::{RelationSource, EDB, IDB},
};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use super::Column;

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
    columns: HashMap<ColumnId, Column>,
}

impl Schema {
    pub fn new(columns: HashMap<ColumnId, Column>) -> Self {
        Self { columns }
    }

    pub fn has_column(&self, k: &ColumnId) -> bool {
        self.columns().contains_key(k)
    }

    pub fn get_column(&self, k: &ColumnId) -> Option<&Column> {
        self.columns.get(k)
    }

    pub fn columns(&self) -> &HashMap<ColumnId, Column> {
        &self.columns
    }
}

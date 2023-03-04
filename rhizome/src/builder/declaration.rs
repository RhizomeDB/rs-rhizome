use anyhow::Result;
use std::{collections::HashMap, marker::PhantomData};

use crate::{
    error::{error, Error},
    id::{ColumnId, RelationId},
    logic::ast::{Column, InnerDeclaration, Schema},
    relation::RelationSource,
    types::{ColumnType, FromType, Type},
};

#[derive(Debug)]
pub struct DeclarationBuilder<T> {
    id: RelationId,
    columns: HashMap<ColumnId, Column>,
    _marker: PhantomData<T>,
}

impl<T> DeclarationBuilder<T>
where
    T: RelationSource,
{
    fn new(id: RelationId) -> Self {
        Self {
            id,
            columns: HashMap::default(),
            _marker: PhantomData::default(),
        }
    }

    fn finalize(self) -> Result<InnerDeclaration<T>> {
        let schema = Schema::new(self.columns);
        let declaration = InnerDeclaration::new(self.id, schema);

        Ok(declaration)
    }

    pub fn build<F>(id: RelationId, f: F) -> Result<InnerDeclaration<T>>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(id))?.finalize()
    }

    pub fn column<C>(mut self, id: &str) -> Result<Self>
    where
        Type: FromType<C>,
    {
        let id = ColumnId::new(id);
        let t = ColumnType::new::<C>();
        let column = Column::new(id, t);

        if self.columns.insert(id, column).is_none() {
            Ok(self)
        } else {
            error(Error::DuplicateSchemaAttributeId(id))
        }
    }
}

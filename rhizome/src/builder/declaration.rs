use anyhow::Result;
use std::{collections::HashMap, marker::PhantomData};

use crate::{
    error::{error, Error},
    id::{ColumnId, RelationId},
    logic::ast::{Column, InnerDeclaration, Schema},
    relation::RelationSource,
    types::{ColumnType, FromType},
};

#[derive(Debug)]
pub struct DeclarationBuilder<T> {
    id: RelationId,
    columns: Vec<(ColumnId, Column)>,
    _marker: PhantomData<T>,
}

impl<T> DeclarationBuilder<T>
where
    T: RelationSource,
{
    fn new(id: RelationId) -> Self {
        Self {
            id,
            columns: Vec::default(),
            _marker: PhantomData::default(),
        }
    }

    fn finalize(self) -> Result<InnerDeclaration<T>> {
        let mut columns = HashMap::default();

        for (column_id, column) in self.columns {
            if let Some(_) = columns.insert(column_id, column) {
                return error(Error::DuplicateSchemaAttributeId(column_id));
            }

            columns.insert(column_id, column);
        }

        let schema = Schema::new(columns);
        let declaration = InnerDeclaration::new(self.id, schema);

        Ok(declaration)
    }

    pub fn build<F>(id: RelationId, f: F) -> Result<InnerDeclaration<T>>
    where
        F: FnOnce(Self) -> Self,
    {
        f(Self::new(id)).finalize()
    }

    pub fn column<C>(mut self, id: &str) -> Self
    where
        ColumnType: FromType<C>,
    {
        let id = ColumnId::new(id);
        let t = ColumnType::new::<C>();
        let column = Column::new(id, t);

        self.columns.push((id, column));

        self
    }
}

use anyhow::Result;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    col::Col,
    error::{error, Error},
    id::{ColId, RelationId},
    logic::ast::InnerDeclaration,
    relation::RelationSource,
    schema::Schema,
    types::{ColType, FromType},
};

#[derive(Debug)]
pub struct DeclarationBuilder<T> {
    id: RelationId,
    cols: Vec<(ColId, Col)>,
    _marker: PhantomData<T>,
}

impl<T> DeclarationBuilder<T>
where
    T: RelationSource,
{
    fn new(id: RelationId) -> Self {
        Self {
            id,
            cols: Vec::default(),
            _marker: PhantomData::default(),
        }
    }

    fn finalize(self) -> Result<InnerDeclaration<T>> {
        let mut cols = HashMap::default();

        for (col_id, col) in self.cols {
            if cols.insert(col_id, col).is_some() {
                return error(Error::DuplicateDeclarationCol(self.id, col_id));
            }

            cols.insert(col_id, col);
        }

        let schema = Schema::new(self.id, cols);
        let declaration = InnerDeclaration::new(self.id, Arc::new(schema));

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
        ColType: FromType<C>,
    {
        let id = ColId::new(id);
        let t = ColType::new::<C>();
        let col = Col::new(id, t);

        self.cols.push((id, col));

        self
    }
}

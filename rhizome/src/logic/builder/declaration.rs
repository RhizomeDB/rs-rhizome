use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    col::Col,
    error::{error, Error},
    id::{ColId, RelationId},
    logic::ast::{Declaration, Schema},
    relation::{DefaultRelation, Relation, Source},
    types::{ColType, IntoColType},
};

#[derive(Debug)]
pub struct DeclarationBuilder<R = DefaultRelation> {
    id: RelationId,
    cols: Vec<(ColId, Col)>,
    source: Source,
    _marker: std::marker::PhantomData<R>,
}

impl<R> DeclarationBuilder<R>
where
    R: Relation + Default + 'static,
{
    fn new(id: RelationId, source: Source) -> Self {
        Self {
            id,
            cols: Vec::default(),
            source,
            _marker: std::marker::PhantomData,
        }
    }

    fn finalize(self) -> Result<Declaration> {
        let mut cols = HashMap::default();

        for (col_id, col) in self.cols {
            if cols.insert(col_id, col).is_some() {
                return error(Error::DuplicateDeclarationCol(self.id, col_id));
            }

            cols.insert(col_id, col);
        }

        let schema = Schema::new(self.id, cols);
        let declaration =
            Declaration::new(self.id, Arc::new(schema), self.source, Box::<R>::default());

        Ok(declaration)
    }

    pub fn build<F>(id: RelationId, source: Source, f: F) -> Result<Declaration>
    where
        F: FnOnce(Self) -> Self,
    {
        f(Self::new(id, source)).finalize()
    }

    pub fn column<C>(mut self, id: &str) -> Self
    where
        C: IntoColType,
    {
        let id = ColId::new(id);
        let t = ColType::new::<C>();
        let col = Col::new(id, t);

        self.cols.push((id, col));

        self
    }
}

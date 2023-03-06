use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, VarId},
    logic::ast::{ColumnValue, Declaration, Negation},
    value::Value,
};

#[derive(Debug)]
pub struct NegationBuilder {
    relation: Arc<Declaration>,
    columns: HashMap<ColumnId, ColumnValue>,
}

impl NegationBuilder {
    fn new(relation: Arc<Declaration>) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
        }
    }

    pub fn build<F>(relation: Arc<Declaration>, f: F) -> Result<Negation>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation))?.finalize()
    }

    fn finalize(self) -> Result<Negation> {
        Ok(Negation::new(self.relation, self.columns))
    }

    pub fn bind<S, T>(mut self, column_id: S, var_id: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);
        let var_id = VarId::new(var_id);

        if !self.relation.schema().has_column(&column_id) {
            return error(Error::UnrecognizedColumnBinding(
                column_id,
                self.relation.id(),
            ));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        self.columns.insert(column_id, ColumnValue::Binding(var_id));

        Ok(self)
    }

    pub fn when<S, T>(mut self, column_id: S, value: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let column_id = ColumnId::new(column_id);
        let value = value.into();

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        column.column_type().check(&value)?;

        self.columns.insert(column_id, ColumnValue::Literal(value));

        Ok(self)
    }
}

use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, VarId},
    logic::ast::{ColumnValue, Declaration, Predicate},
    types::ColumnType,
    value::Value,
};

#[derive(Debug)]
pub struct PredicateBuilder<'a> {
    relation: Arc<Declaration>,
    columns: HashMap<ColumnId, ColumnValue>,
    bound_vars: &'a mut HashMap<VarId, ColumnType>,
}

impl<'a> PredicateBuilder<'a> {
    fn new(relation: Arc<Declaration>, bound_vars: &'a mut HashMap<VarId, ColumnType>) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
            bound_vars,
        }
    }

    pub fn build<F>(
        relation: Arc<Declaration>,
        bound_vars: &'a mut HashMap<VarId, ColumnType>,
        f: F,
    ) -> Result<Predicate>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation, bound_vars))?.finalize()
    }

    pub fn finalize(self) -> Result<Predicate> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        let predicate = Predicate::new(self.relation, self.columns);

        Ok(predicate)
    }

    pub fn bind<S, T>(mut self, column_id: S, variable_id: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);
        let variable_id = VarId::new(variable_id);

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()))
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        if let Some(bound_type) = self.bound_vars.get(&variable_id) {
            if bound_type != column.column_type() {
                return error(Error::VariableTypeConflict(
                    variable_id,
                    *column.column_type(),
                    *bound_type,
                ));
            }
        } else {
            self.bound_vars.insert(variable_id, *column.column_type());
        }

        self.columns
            .insert(column_id, ColumnValue::Binding(variable_id));

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
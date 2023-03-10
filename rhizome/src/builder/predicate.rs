use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, VarId},
    logic::ast::{ColumnValue, Declaration, Predicate, Var},
    types::Type,
    value::Value,
};

#[derive(Debug, Default)]
pub struct PredicateBuilder {
    pub(super) bindings: Vec<(ColumnId, ColumnValue)>,
}

impl PredicateBuilder {
    pub fn new() -> Self {
        Self {
            bindings: Vec::default(),
        }
    }
    pub fn finalize(
        self,
        relation: Arc<Declaration>,
        bound_vars: &mut HashMap<VarId, Type>,
    ) -> Result<Predicate> {
        let mut columns = HashMap::default();

        for (column_id, column_value) in self.bindings {
            let Some(column) = relation.schema().get_column(&column_id) else {
                return error(Error::UnrecognizedColumnBinding(relation.id(), column_id));
            };

            if columns.contains_key(&column_id) {
                return error(Error::ConflictingColumnBinding(relation.id(), column_id));
            }

            match &column_value {
                ColumnValue::Literal(val) => {
                    if column.column_type().check(val).is_err() {
                        return error(Error::ColumnValueTypeConflict(
                            relation.id(),
                            column_id,
                            column_value,
                            *column.column_type(),
                        ));
                    }
                }
                ColumnValue::Binding(var) => {
                    if let Some(downcasted) = column.column_type().downcast(&var.typ()) {
                        bound_vars.insert(var.id(), downcasted);
                    } else {
                        return error(Error::ColumnValueTypeConflict(
                            relation.id(),
                            column_id,
                            ColumnValue::Binding(*var),
                            *column.column_type(),
                        ));
                    }
                }
            }

            columns.insert(column_id, column_value);
        }

        let predicate = Predicate::new(relation, columns);

        Ok(predicate)
    }

    pub fn bind<S>(mut self, column_id: S, var: &Var) -> Self
    where
        S: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);

        self.bindings.push((column_id, ColumnValue::Binding(*var)));

        self
    }

    pub fn when<S, T>(mut self, column_id: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let column_id = ColumnId::new(column_id);
        let value = value.into();

        self.bindings.push((column_id, ColumnValue::Literal(value)));

        self
    }
}

use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, VarId},
    logic::ast::{ColumnValue, Declaration, Predicate, Var},
    types::Type,
    value::Value,
};

#[derive(Debug)]
pub struct PredicateBuilder {
    bindings: Vec<(ColumnId, ColumnValue)>,
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
                return error(Error::UnrecognizedColumnBinding(column_id, relation.id()));
            };

            if columns.contains_key(&column_id) {
                return error(Error::ConflictingColumnBinding(column_id));
            }

            match &column_value {
                ColumnValue::Literal(value) => column.column_type().check(&value)?,
                ColumnValue::Binding(var) => {
                    if let Some(downcasted) = column.column_type().downcast(&var.typ()) {
                        bound_vars.insert(var.id(), downcasted);
                    } else {
                        return error(Error::VariableTypeConflict(
                            var.id(),
                            *column.column_type(),
                            var.typ(),
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

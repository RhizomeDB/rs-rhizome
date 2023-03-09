use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::ColumnId,
    logic::ast::{ColumnValue, Declaration, Negation, Var},
    value::Value,
};

#[derive(Debug, Default)]
pub struct NegationBuilder {
    pub(super) bindings: Vec<(ColumnId, ColumnValue)>,
}

impl NegationBuilder {
    pub fn finalize(self, relation: Arc<Declaration>) -> Result<Negation> {
        let mut columns = HashMap::default();

        for (column_id, column_value) in self.bindings {
            let Some(column) = relation.schema().get_column(&column_id) else {
                return error(Error::UnrecognizedColumnBinding(column_id, relation.id()));
            };

            if columns.contains_key(&column_id) {
                return error(Error::ConflictingColumnBinding(column_id));
            }

            match &column_value {
                ColumnValue::Literal(value) => column.column_type().check(value)?,
                ColumnValue::Binding(var) => {
                    if let None = column.column_type().downcast(&var.typ()) {
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

        let negation = Negation::new(relation, columns);

        Ok(negation)
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

use anyhow::Result;
use std::collections::HashMap;

use crate::{
    error::{error, Error},
    id::ColumnId,
    logic::ast::{ColumnValue, Declaration, Fact},
    value::Value,
};

use super::atom_args::{AtomArg, AtomArgs};

#[derive(Debug)]
pub struct FactBuilder<'a> {
    relation: &'a Declaration,
    bindings: Vec<(ColumnId, ColumnValue)>,
}

impl<'a> FactBuilder<'a> {
    fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            bindings: Vec::default(),
        }
    }

    pub fn build<F>(relation: &'a Declaration, f: F) -> Result<Fact>
    where
        F: FnOnce(Self) -> Self,
    {
        f(Self::new(relation)).finalize()
    }

    pub fn finalize(self) -> Result<Fact> {
        let mut columns = HashMap::default();

        for (column_id, column_value) in self.bindings {
            match column_value {
                ColumnValue::Literal(val) => {
                    let Some(column) = self.relation.schema().get_column(&column_id) else {
                    return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
                };

                    if columns.contains_key(&column_id) {
                        return error(Error::ConflictingColumnBinding(column_id));
                    }

                    column.column_type().check(&val)?;

                    columns.insert(column_id, val);
                }
                ColumnValue::Binding(var) => {
                    return error(Error::NonGroundFact(column_id, var.id()));
                }
            }
        }

        for column_id in self.relation.schema().columns().keys() {
            if !columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        match self.relation {
            Declaration::EDB(inner) => error(Error::ClauseHeadEDB(inner.id())),
            Declaration::IDB(inner) => {
                let fact = Fact::new(inner.id(), columns);

                Ok(fact)
            }
        }
    }

    pub fn bind<T, A>(mut self, bindings: T) -> Self
    where
        T: AtomArgs<A>,
    {
        for (id, value) in T::into_columns(bindings) {
            self.bindings.push((id, value));
        }

        self
    }

    pub fn bind_one<T>(mut self, binding: T) -> Self
    where
        T: AtomArg<Value>,
    {
        let (id, value) = binding.into_column();

        self.bindings.push((id, value));

        self
    }
}

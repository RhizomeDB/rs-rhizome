use anyhow::Result;
use std::collections::HashMap;

use crate::{
    error::{error, Error},
    id::ColumnId,
    logic::ast::{Declaration, Fact},
    value::Value,
};

#[derive(Debug)]
pub struct FactBuilder<'a> {
    relation: &'a Declaration,
    bindings: Vec<(ColumnId, Value)>,
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
            let Some(column) = self.relation.schema().get_column(&column_id) else {
                return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
            };

            if columns.contains_key(&column_id) {
                return error(Error::ConflictingColumnBinding(column_id));
            }

            column.column_type().check(&column_value)?;

            columns.insert(column_id, column_value);
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

    pub fn set<S, T>(mut self, id: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let id = ColumnId::new(id);
        let value = value.into();

        self.bindings.push((id, value));

        self
    }
}

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
    columns: HashMap<ColumnId, Value>,
}

impl<'a> FactBuilder<'a> {
    fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
        }
    }

    pub fn build<F>(relation: &'a Declaration, f: F) -> Result<Fact>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation))?.finalize()
    }

    pub fn finalize(self) -> Result<Fact> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        match self.relation {
            Declaration::EDB(inner) => error(Error::ClauseHeadEDB(inner.id())),
            Declaration::IDB(inner) => {
                let fact = Fact::new(inner.id(), self.columns);

                Ok(fact)
            }
        }
    }

    pub fn set<S, T>(mut self, id: S, value: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let id = ColumnId::new(id);
        let value = value.into();

        let Some(column) = self.relation.schema().get_column(&id) else {
            return error(Error::UnrecognizedColumnBinding(id, self.relation.id()));
        };

        if self.columns.contains_key(&id) {
            return error(Error::ConflictingColumnBinding(id));
        }

        column.column_type().check(&value)?;

        self.columns.insert(id, value);

        Ok(self)
    }
}

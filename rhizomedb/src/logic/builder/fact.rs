use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::ast::{Declaration, Fact},
    relation::Source,
};

use super::{atom_binding::AtomBinding, atom_bindings::AtomBindings};

#[derive(Debug)]
pub struct FactBuilder {
    relation: Arc<Declaration>,
    bindings: Vec<(ColId, ColVal)>,
}

impl FactBuilder {
    fn new(relation: Arc<Declaration>) -> Self {
        Self {
            relation,
            bindings: Vec::default(),
        }
    }

    pub fn build<F>(relation: Arc<Declaration>, f: F) -> Result<Fact>
    where
        F: FnOnce(Self) -> Self,
    {
        f(Self::new(relation)).finalize()
    }

    pub fn finalize(self) -> Result<Fact> {
        let schema = self.relation.schema();
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings {
            match col_val {
                ColVal::Lit(val) => {
                    let Some(col) = schema.get_col(&col_id) else {
                        return error(Error::UnrecognizedColumnBinding(self.relation.id(), col_id));
                    };

                    if cols.contains_key(&col_id) {
                        return error(Error::ConflictingColumnBinding(self.relation.id(), col_id));
                    }

                    if col.col_type().check(&val).is_err() {
                        return error(Error::ColumnValueTypeConflict(
                            self.relation.id(),
                            col_id,
                            ColVal::Lit(val),
                            *col.col_type(),
                        ));
                    };

                    cols.insert(col_id, val);
                }
                ColVal::Binding(var) => {
                    return error(Error::NonGroundFact(self.relation.id(), col_id, var.id()));
                }
            }
        }

        for col_id in self.relation.schema().cols().keys() {
            if !cols.contains_key(col_id) {
                return error(Error::ColumnMissing(self.relation.id(), *col_id));
            }
        }

        match self.relation.source() {
            Source::Edb => error(Error::ClauseHeadEDB(self.relation.id())),
            Source::Idb => {
                let fact = Fact::new(self.relation.id(), cols);

                Ok(fact)
            }
        }
    }

    pub fn bind<T>(mut self, bindings: T) -> Self
    where
        T: AtomBindings,
    {
        bindings.bind(&mut self.bindings);

        self
    }

    pub fn bind_one<T>(mut self, binding: T) -> Self
    where
        T: AtomBinding,
    {
        let (id, value) = binding.into_pair();

        self.bindings.push((id, value));

        self
    }
}

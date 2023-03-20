use anyhow::Result;
use std::collections::HashMap;

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::ast::{Declaration, Fact},
    value::Val,
};

use super::atom_args::{AtomArg, AtomArgs};

#[derive(Debug)]
pub struct FactBuilder<'a> {
    relation: &'a Declaration,
    bindings: Vec<(ColId, ColVal)>,
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

        match self.relation {
            Declaration::Edb(inner) => error(Error::ClauseHeadEDB(inner.id())),
            Declaration::Idb(inner) => {
                let fact = Fact::new(inner.id(), cols);

                Ok(fact)
            }
        }
    }

    pub fn bind<T, A>(mut self, bindings: T) -> Self
    where
        T: AtomArgs<A>,
    {
        for (id, value) in T::into_cols(bindings) {
            self.bindings.push((id, value));
        }

        self
    }

    pub fn bind_one<T>(mut self, binding: T) -> Self
    where
        T: AtomArg<Val>,
    {
        let (id, value) = binding.into_col();

        self.bindings.push((id, value));

        self
    }
}

use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::ast::Declaration,
    types::Type,
    value::Val,
    var::Var,
};

use super::atom_args::{AtomArg, AtomArgs};

#[derive(Debug)]
pub struct RuleHeadBuilder<'a> {
    relation: &'a Declaration,
    bindings: Vec<(ColId, ColVal)>,
}

impl<'a> RuleHeadBuilder<'a> {
    pub fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            bindings: Vec::default(),
        }
    }

    pub fn finalize(self, bound_vars: &HashMap<Var, Type>) -> Result<HashMap<ColId, ColVal>> {
        let schema = self.relation.schema();
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings {
            let Some(col) = schema.get_col(&col_id) else {
                return error(Error::UnrecognizedColumnBinding(self.relation.id(), col_id));
            };

            if cols.contains_key(&col_id) {
                return error(Error::ConflictingColumnBinding(self.relation.id(), col_id));
            }

            match &col_val {
                ColVal::Lit(val) => {
                    if col.col_type().check(val).is_err() {
                        return error(Error::ColumnValueTypeConflict(
                            self.relation.id(),
                            col_id,
                            col_val,
                            *col.col_type(),
                        ));
                    }
                }
                ColVal::Binding(var) => {
                    if col.col_type().downcast(&var.typ()).is_none() {
                        return error(Error::ColumnValueTypeConflict(
                            self.relation.id(),
                            col_id,
                            ColVal::Binding(*var),
                            *col.col_type(),
                        ));
                    }
                }
            }

            cols.insert(col_id, col_val);
        }

        for col_id in self.relation.schema().cols().keys() {
            if !cols.contains_key(col_id) {
                return error(Error::ColumnMissing(self.relation.id(), *col_id));
            }
        }

        for (col_id, val) in &cols {
            if let ColVal::Binding(var) = val {
                if !bound_vars.contains_key(var) {
                    return error(Error::ClauseNotRangeRestricted(*col_id, var.id()));
                }
            }
        }

        Ok(cols)
    }

    pub fn set<S, T>(mut self, id: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Val>,
    {
        let id = ColId::new(id);
        let value = Arc::new(value.into());

        self.bindings.push((id, ColVal::Lit(value)));

        self
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

    pub fn bind_one<T, A>(mut self, binding: T) -> Self
    where
        T: AtomArg<A>,
    {
        let (id, value) = binding.into_col();

        self.bindings.push((id, value));

        self
    }
}

use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::ColId,
    logic::ast::{ColVal, Declaration, Predicate, Var},
    types::Type,
    value::Val,
};

#[derive(Debug, Default)]
pub struct PredicateBuilder {
    pub(super) bindings: Vec<(ColId, ColVal)>,
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
        bound_vars: &mut HashMap<Var, Type>,
    ) -> Result<Predicate> {
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings {
            let Some(col) = relation.schema().get_col(&col_id) else {
                return error(Error::UnrecognizedColumnBinding(relation.id(), col_id));
            };

            if cols.contains_key(&col_id) {
                return error(Error::ConflictingColumnBinding(relation.id(), col_id));
            }

            match &col_val {
                ColVal::Lit(val) => {
                    if col.col_type().check(val).is_err() {
                        return error(Error::ColumnValueTypeConflict(
                            relation.id(),
                            col_id,
                            col_val,
                            *col.col_type(),
                        ));
                    }
                }
                ColVal::Binding(var) => {
                    if let Some(downcasted) = col.col_type().downcast(&var.typ()) {
                        bound_vars.insert(*var, downcasted);
                    } else {
                        return error(Error::ColumnValueTypeConflict(
                            relation.id(),
                            col_id,
                            ColVal::Binding(*var),
                            *col.col_type(),
                        ));
                    }
                }
            }

            cols.insert(col_id, col_val);
        }

        let predicate = Predicate::new(relation, cols);

        Ok(predicate)
    }

    pub fn bind<S>(mut self, col_id: S, var: &Var) -> Self
    where
        S: AsRef<str>,
    {
        let col_id = ColId::new(col_id);

        self.bindings.push((col_id, ColVal::Binding(*var)));

        self
    }

    pub fn when<S, T>(mut self, col_id: S, val: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Val>,
    {
        let col_id = ColId::new(col_id);
        let val = val.into();

        self.bindings.push((col_id, ColVal::Lit(val)));

        self
    }
}

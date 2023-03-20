use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::ast::{Declaration, Negation},
    value::Val,
    var::Var,
};

#[derive(Debug, Default)]
pub struct NegationBuilder {
    pub(super) bindings: Vec<(ColId, ColVal)>,
}

impl NegationBuilder {
    pub fn finalize(self, relation: Arc<Declaration>) -> Result<Negation> {
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings {
            let schema = relation.schema();

            let Some(col) = schema.get_col(&col_id) else {
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
                    if col.col_type().downcast(&var.typ()).is_none() {
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

        let negation = Negation::new(relation, cols);

        Ok(negation)
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
        let val = Arc::new(val.into());

        self.bindings.push((col_id, ColVal::Lit(val)));

        self
    }
}

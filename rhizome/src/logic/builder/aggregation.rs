use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use crate::{
    aggregation_function::AggregationFunction,
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::ast::{Aggregation, Declaration},
    types::Type,
    var::Var,
};

#[derive(Debug)]
pub(crate) struct AggregationBuilder {
    pub(super) f: AggregationFunction,
    pub(super) target: Var,
    pub(super) bindings: Vec<(ColId, ColVal)>,
}

impl AggregationBuilder {
    pub(crate) fn new(f: AggregationFunction, target: Var) -> Self {
        Self {
            f,
            target,
            bindings: Vec::default(),
        }
    }
    pub(crate) fn finalize(
        self,
        relation: Arc<Declaration>,
        bound_vars: &mut HashMap<Var, Type>,
    ) -> Result<Aggregation> {
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

        let aggregation = Aggregation::new(self.f, relation, self.target, cols);

        Ok(aggregation)
    }
}

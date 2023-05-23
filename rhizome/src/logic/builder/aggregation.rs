use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::{ColId, VarId},
    logic::{
        ast::{Aggregation, Declaration},
        AggregationClosure,
    },
    types::ColType,
    value::Val,
    var::Var,
};

pub(crate) struct AggregationBuilder {
    pub(super) init: Option<Val>,
    pub(super) target: Var,
    pub(super) vars: Vec<Var>,
    pub(super) bindings: Vec<(ColId, ColVal)>,
    pub(super) f: Arc<dyn AggregationClosure>,
}

impl AggregationBuilder {
    pub(crate) fn new(init: Option<Val>, target: Var, f: Arc<dyn AggregationClosure>) -> Self {
        Self {
            init,
            target,
            f,
            vars: Vec::default(),
            bindings: Vec::default(),
        }
    }
    pub(crate) fn finalize(
        self,
        relation: Arc<Declaration>,
        bound_vars: &mut HashMap<VarId, ColType>,
    ) -> Result<Aggregation> {
        if bound_vars
            .insert(self.target.id(), self.target.typ())
            .is_some()
        {
            return error(Error::AggregationBoundTarget(self.target.id()));
        }

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
                    if *var == self.target {
                        return error(Error::AggregationGroupByTarget(var.id()));
                    }

                    if col.col_type().unify(&var.typ()).is_err() {
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

        let aggregation =
            Aggregation::new(self.target, self.vars, self.init, relation, cols, self.f);

        Ok(aggregation)
    }
}

impl Debug for AggregationBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregationBuilder")
            .field("target", &self.target)
            .field("vars", &self.vars)
            .field("bindings", &self.bindings)
            .finish()
    }
}

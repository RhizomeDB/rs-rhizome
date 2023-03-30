use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::ColId,
    logic::{
        ast::{Declaration, Reduce},
        ReduceClosure,
    },
    types::Type,
    value::Val,
    var::Var,
};

pub(crate) struct ReduceBuilder {
    pub(super) init: Val,
    pub(super) target: Var,
    pub(super) vars: Vec<Var>,
    pub(super) bindings: Vec<(ColId, ColVal)>,
    pub(super) f: Arc<dyn ReduceClosure>,
}

impl ReduceBuilder {
    pub(crate) fn new(init: Val, target: Var, f: Arc<dyn ReduceClosure>) -> Self {
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
        bound_vars: &mut HashMap<Var, Type>,
    ) -> Result<Reduce> {
        if bound_vars.insert(self.target, self.target.typ()).is_some() {
            return error(Error::ReduceBoundTarget(self.target.id()));
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
                        return error(Error::ReduceGroupByTarget(var.id()));
                    }

                    if !bound_vars.contains_key(var) && !self.vars.contains(var) {
                        return error(Error::ReduceUnboundGroupBy(var.id(), col_id, relation.id()));
                    }

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

        let reduce = Reduce::new(self.target, self.vars, self.init, relation, cols, self.f);

        Ok(reduce)
    }
}

impl Debug for ReduceBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregationBuilder")
            .field("target", &self.target)
            .field("vars", &self.vars)
            .field("bindings", &self.bindings)
            .finish()
    }
}

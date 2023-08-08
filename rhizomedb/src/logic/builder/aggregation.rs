use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::{ColId, VarId},
    logic::ast::{Aggregation, Declaration},
    types::ColType,
    var::Var,
    AtomBinding,
};

use crate::aggregation::AggregateWrapper;

pub struct AggregationBuilder {
    pub(super) target: Var,
    pub(super) vars: Vec<Var>,
    pub(super) bindings: RefCell<Vec<(ColId, ColVal)>>,
    pub(super) agg: Arc<dyn AggregateWrapper>,
}

impl AggregationBuilder {
    pub(crate) fn new(target: Var, f: Arc<dyn AggregateWrapper>) -> Self {
        Self {
            target,
            agg: f,
            vars: Default::default(),
            bindings: Default::default(),
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

        for (col_id, col_val) in self.bindings.into_inner() {
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

        let aggregation = Aggregation::new(self.target, self.vars, relation, cols, self.agg);

        Ok(aggregation)
    }

    pub fn bind_one<T>(&self, binding: T) -> Result<()>
    where
        T: AtomBinding,
    {
        let (id, val) = binding.into_pair();

        self.bindings.borrow_mut().push((id, val));

        Ok(())
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

use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::{ColId, VarId},
    logic::ast::Declaration,
    types::ColType,
    value::Val,
};

use super::{atom_binding::AtomBinding, atom_bindings::AtomBindings};

#[derive(Debug)]
pub struct RuleHeadBuilder {
    relation: Arc<Declaration>,
    bindings: RefCell<Vec<(ColId, ColVal)>>,
}

impl RuleHeadBuilder {
    pub fn new(relation: Arc<Declaration>) -> Self {
        Self {
            relation,
            bindings: RefCell::default(),
        }
    }

    pub fn finalize(
        self,
        bound_vars: &mut HashMap<VarId, ColType>,
    ) -> Result<HashMap<ColId, ColVal>> {
        let schema = self.relation.schema();
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings.into_inner() {
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
                    if let Some(bound_type) = bound_vars.get(&var.id()) {
                        if let Ok(unified_type) = bound_type
                            .unify(col.col_type())
                            .and_then(|t| t.unify(&var.typ()))
                        {
                            bound_vars.insert(var.id(), unified_type);
                        } else {
                            return error(Error::ColumnValueTypeConflict(
                                self.relation.id(),
                                col_id,
                                ColVal::Binding(*var),
                                *col.col_type(),
                            ));
                        }
                    } else {
                        return error(Error::ClauseNotRangeRestricted(col_id, var.id()));
                    }

                    if !bound_vars.contains_key(&var.id()) {
                        return error(Error::ClauseNotRangeRestricted(col_id, var.id()));
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

        Ok(cols)
    }

    pub fn set<S, T>(&self, id: S, value: T) -> Result<()>
    where
        S: AsRef<str>,
        T: Into<Val>,
    {
        let id = ColId::new(id);
        let value = Arc::new(value.into());

        self.bindings.borrow_mut().push((id, ColVal::Lit(value)));

        Ok(())
    }

    pub fn bind<T>(&self, bindings: T) -> Result<()>
    where
        T: AtomBindings,
    {
        bindings.bind(&mut self.bindings.borrow_mut());

        Ok(())
    }

    pub fn bind_one<T>(&self, binding: T) -> Result<()>
    where
        T: AtomBinding,
    {
        let (id, value) = binding.into_pair();

        self.bindings.borrow_mut().push((id, value));

        Ok(())
    }
}

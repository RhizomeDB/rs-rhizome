use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use crate::{
    aggregation::AggregateWrapper,
    id::{ColId, VarId},
    predicate::PredicateWrapper,
    var::Var,
};

use super::{CidValue, Declaration};
use crate::col_val::ColVal;

#[derive(Debug)]
pub enum BodyTerm {
    VarPredicate(VarPredicate),
    RelPredicate(RelPredicate),
    Negation(Negation),
    Aggregation(Aggregation),
}

#[derive(Debug, Clone)]
pub struct RelPredicate {
    relation: Arc<Declaration>,
    cid: Option<CidValue>,
    args: HashMap<ColId, ColVal>,
}

impl RelPredicate {
    pub fn new(
        relation: Arc<Declaration>,
        cid: Option<CidValue>,
        args: HashMap<ColId, ColVal>,
    ) -> Self {
        Self {
            relation,
            cid,
            args,
        }
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
    }

    pub fn cid(&self) -> &Option<CidValue> {
        &self.cid
    }

    pub fn args(&self) -> &HashMap<ColId, ColVal> {
        &self.args
    }

    pub fn vars(&self) -> HashSet<&Var> {
        self.args
            .iter()
            .filter_map(|(_, v)| match v {
                ColVal::Lit(_) => None,
                ColVal::Binding(var) => Some(var),
            })
            .collect()
    }

    pub fn bound_vars(&self, bindings: &HashSet<VarId>) -> HashSet<VarId> {
        let mut result = HashSet::default();

        if let Some(CidValue::Var(var)) = self.cid() {
            if bindings.contains(&var.id()) {
                result.insert(var.id());
            }
        }

        for var in self.vars() {
            if bindings.contains(&var.id()) {
                result.insert(var.id());
            }
        }

        result
    }
}

#[derive(Debug, Clone)]
pub struct Negation {
    relation: Arc<Declaration>,
    args: HashMap<ColId, ColVal>,
}

impl Negation {
    pub fn new(relation: Arc<Declaration>, args: HashMap<ColId, ColVal>) -> Self {
        Self { relation, args }
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
    }

    pub fn args(&self) -> &HashMap<ColId, ColVal> {
        &self.args
    }

    pub fn is_vars_bound(&self, bindings: &HashSet<VarId>) -> bool {
        self.vars().iter().all(|var| bindings.contains(&var.id()))
    }

    pub fn vars(&self) -> HashSet<&Var> {
        self.args
            .iter()
            .filter_map(|(_, v)| match v {
                ColVal::Lit(_) => None,
                ColVal::Binding(var) => Some(var),
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct VarPredicate {
    vars: Vec<Var>,
    f: Arc<dyn PredicateWrapper>,
}

impl VarPredicate {
    pub fn new(vars: Vec<Var>, f: Arc<dyn PredicateWrapper>) -> Self {
        Self { vars, f }
    }

    pub fn vars(&self) -> &Vec<Var> {
        &self.vars
    }

    pub fn f(&self) -> Arc<dyn PredicateWrapper> {
        Arc::clone(&self.f)
    }

    pub fn is_vars_bound(&self, bindings: &HashSet<VarId>) -> bool {
        self.vars().iter().all(|var| bindings.contains(&var.id()))
    }
}

impl Debug for VarPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VarPredicate")
            .field("vars", &self.vars)
            .finish()
    }
}

#[derive(Clone)]
pub struct Aggregation {
    target: Var,
    vars: Vec<Var>,
    relation: Arc<Declaration>,
    group_by_cols: HashMap<ColId, ColVal>,
    agg: Arc<dyn AggregateWrapper>,
}

impl Aggregation {
    pub fn new(
        target: Var,
        vars: Vec<Var>,
        relation: Arc<Declaration>,
        group_by_cols: HashMap<ColId, ColVal>,
        f: Arc<dyn AggregateWrapper>,
    ) -> Self {
        Self {
            target,
            vars,
            relation,
            group_by_cols,
            agg: f,
        }
    }

    pub fn target(&self) -> &Var {
        &self.target
    }

    pub fn vars(&self) -> &Vec<Var> {
        &self.vars
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
    }

    pub fn group_by_cols(&self) -> &HashMap<ColId, ColVal> {
        &self.group_by_cols
    }

    pub fn agg(&self) -> Arc<dyn AggregateWrapper> {
        Arc::clone(&self.agg)
    }

    pub fn bound_vars(&self, bindings: &HashSet<VarId>) -> HashSet<VarId> {
        self.vars()
            .iter()
            .filter(|v| bindings.contains(&v.id()))
            .map(|v| v.id())
            .collect()
    }

    pub fn is_vars_bound<T>(&self, bindings: &im::HashMap<VarId, T>) -> bool {
        self.vars()
            .iter()
            .all(|var| bindings.contains_key(&var.id()))
    }
}

impl Debug for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Aggregation")
            .field("target", &self.target)
            .field("vars", &self.vars)
            .field("relation", &self.relation)
            .field("group_by_cols", &self.group_by_cols)
            .finish()
    }
}

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use anyhow::Result;

use crate::{
    error::Error,
    id::{ColId, LinkId, VarId},
    logic::{ReduceClosure, VarClosure},
    value::Val,
    var::Var,
};

use super::{CidValue, Declaration};
use crate::col_val::ColVal;

#[derive(Debug)]
pub enum BodyTerm {
    VarPredicate(VarPredicate),
    RelPredicate(RelPredicate),
    Negation(Negation),
    GetLink(GetLink),
    Reduce(Reduce),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RelPredicate {
    relation: Arc<Declaration>,
    args: HashMap<ColId, ColVal>,
}

impl RelPredicate {
    pub fn new(relation: Arc<Declaration>, args: HashMap<ColId, ColVal>) -> Self {
        Self { relation, args }
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
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
        self.vars()
            .into_iter()
            .filter(|v| bindings.contains(&v.id()))
            .map(|v| v.id())
            .collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetLink {
    cid: CidValue,
    link_id: LinkId,
    link_value: CidValue,
}

impl GetLink {
    pub fn new(cid: CidValue, args: Vec<(LinkId, CidValue)>) -> Result<Self> {
        let links: Vec<_> = args.into_iter().collect();

        // TODO: Support multiple links; see https://github.com/RhizomeDB/rs-rhizome/issues/22
        debug_assert!(links.len() == 1);

        let link = links
            .get(0)
            .ok_or_else(|| Error::InternalRhizomeError("link not found".to_owned()))?;

        Ok(Self {
            cid,
            link_id: link.0,
            link_value: link.1,
        })
    }

    pub fn cid(&self) -> CidValue {
        self.cid
    }

    pub fn link_id(&self) -> LinkId {
        self.link_id
    }

    pub fn link_value(&self) -> CidValue {
        self.link_value
    }

    pub fn len_bound_args(&self, bindings: &HashSet<VarId>) -> usize {
        let mut len = 0;

        if let CidValue::Var(var) = self.cid() {
            if bindings.contains(&var.id()) {
                len += 1;
            }
        } else {
            len += 1;
        }

        if let CidValue::Var(var) = self.link_value() {
            if bindings.contains(&var.id()) {
                len += 1;
            }
        } else {
            len += 1;
        }

        len
    }
}

#[derive(Clone)]
pub struct VarPredicate {
    vars: Vec<Var>,
    f: Arc<dyn VarClosure>,
}

impl VarPredicate {
    pub fn new(vars: Vec<Var>, f: Arc<dyn VarClosure>) -> Self {
        Self { vars, f }
    }

    pub fn vars(&self) -> &Vec<Var> {
        &self.vars
    }

    pub fn f(&self) -> Arc<dyn VarClosure> {
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
pub struct Reduce {
    target: Var,
    vars: Vec<Var>,
    init: Val,
    relation: Arc<Declaration>,
    group_by_cols: HashMap<ColId, ColVal>,
    f: Arc<dyn ReduceClosure>,
}

impl Reduce {
    pub fn new(
        target: Var,
        vars: Vec<Var>,
        init: Val,
        relation: Arc<Declaration>,
        group_by_cols: HashMap<ColId, ColVal>,
        f: Arc<dyn ReduceClosure>,
    ) -> Self {
        Self {
            target,
            vars,
            init,
            relation,
            group_by_cols,
            f,
        }
    }

    pub fn target(&self) -> &Var {
        &self.target
    }

    pub fn vars(&self) -> &Vec<Var> {
        &self.vars
    }

    pub fn init(&self) -> &Val {
        &self.init
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
    }

    pub fn group_by_cols(&self) -> &HashMap<ColId, ColVal> {
        &self.group_by_cols
    }

    pub fn f(&self) -> Arc<dyn ReduceClosure> {
        Arc::clone(&self.f)
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

impl Debug for Reduce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reduce")
            .field("target", &self.target)
            .field("vars", &self.vars)
            .field("relation", &self.relation)
            .field("group_by_cols", &self.group_by_cols)
            .finish()
    }
}

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use anyhow::Result;
use derive_more::{From, IsVariant};

use crate::{
    error::Error,
    id::{ColId, LinkId, VarId},
    logic::{ReduceClosure, VarClosure},
    value::Val,
    var::Var,
};

use super::{CidValue, Declaration, Polarity};
use crate::col_val::ColVal;

#[derive(Debug, From, IsVariant)]
pub enum BodyTerm {
    VarPredicate(VarPredicate),
    RelPredicate(RelPredicate),
    Negation(Negation),
    GetLink(GetLink),
    Reduce(Reduce),
}

impl BodyTerm {
    pub fn depends_on(&self) -> Vec<Arc<Declaration>> {
        match self {
            BodyTerm::RelPredicate(inner) => vec![inner.relation()],
            BodyTerm::Negation(inner) => vec![inner.relation()],
            BodyTerm::GetLink(_) => vec![],
            BodyTerm::VarPredicate(_) => vec![],
            BodyTerm::Reduce(_) => vec![],
        }
    }

    pub fn polarity(&self) -> Option<Polarity> {
        match self {
            BodyTerm::RelPredicate(_) => Some(Polarity::Positive),
            BodyTerm::Negation(_) => Some(Polarity::Negative),
            BodyTerm::GetLink(_) => None,
            BodyTerm::VarPredicate(_) => None,
            BodyTerm::Reduce(_) => Some(Polarity::Negative),
        }
    }
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

    pub fn is_vars_bound<T>(&self, bindings: &im::HashMap<VarId, T>) -> bool {
        self.vars()
            .iter()
            .all(|var| bindings.contains_key(&var.id()))
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
}

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

    pub fn is_vars_bound<T>(&self, bindings: &im::HashMap<VarId, T>) -> bool {
        self.vars()
            .iter()
            .all(|var| bindings.contains_key(&var.id()))
    }
}

impl Debug for VarPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VarPredicate")
            .field("vars", &self.vars)
            .finish()
    }
}

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

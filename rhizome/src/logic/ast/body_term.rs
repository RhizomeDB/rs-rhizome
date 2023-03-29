use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use derive_more::{From, IsVariant};

use crate::{
    aggregation_function::AggregationFunction,
    id::{ColId, LinkId},
    logic::VarClosure,
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
    Aggregation(Aggregation),
}

impl BodyTerm {
    pub fn depends_on(&self) -> Vec<Arc<Declaration>> {
        match self {
            BodyTerm::RelPredicate(inner) => vec![inner.relation()],
            BodyTerm::Negation(inner) => vec![inner.relation()],
            BodyTerm::GetLink(_) => vec![],
            BodyTerm::VarPredicate(_) => vec![],
            BodyTerm::Aggregation(inner) => vec![inner.relation()],
        }
    }

    pub fn polarity(&self) -> Option<Polarity> {
        match self {
            BodyTerm::RelPredicate(_) => Some(Polarity::Positive),
            BodyTerm::Negation(_) => Some(Polarity::Negative),
            BodyTerm::GetLink(_) => None,
            BodyTerm::VarPredicate(_) => None,
            BodyTerm::Aggregation(_) => Some(Polarity::Negative),
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

    pub fn is_vars_bound<T>(&self, bindings: &im::HashMap<Var, T>) -> bool {
        self.vars().iter().all(|var| bindings.contains_key(var))
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
    pub fn new(cid: CidValue, args: Vec<(LinkId, CidValue)>) -> Self {
        let links: Vec<_> = args.into_iter().collect();

        // TODO: Support multiple links
        assert!(links.len() == 1);

        let link_id = links.get(0).unwrap().0;
        let link_value = links.get(0).unwrap().1;

        Self {
            cid,
            link_id,
            link_value,
        }
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

    pub fn is_vars_bound<T>(&self, bindings: &im::HashMap<Var, T>) -> bool {
        self.vars.iter().all(|var| bindings.contains_key(var))
    }
}

impl Debug for VarPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VarPredicate")
            .field("vars", &self.vars)
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Aggregation {
    function: AggregationFunction,
    relation: Arc<Declaration>,
    group_by_cols: HashMap<ColId, ColVal>,
    target_var: Var,
}

impl Aggregation {
    pub fn new(
        function: AggregationFunction,
        relation: Arc<Declaration>,
        target_var: Var,
        group_by_cols: HashMap<ColId, ColVal>,
    ) -> Self {
        Self {
            function,
            relation,
            target_var,
            group_by_cols,
        }
    }

    pub fn function(&self) -> AggregationFunction {
        self.function
    }

    pub fn relation(&self) -> Arc<Declaration> {
        Arc::clone(&self.relation)
    }

    pub fn group_by_cols(&self) -> &HashMap<ColId, ColVal> {
        &self.group_by_cols
    }

    pub fn target_var(&self) -> &Var {
        &self.target_var
    }
}

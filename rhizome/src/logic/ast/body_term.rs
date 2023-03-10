use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use derive_more::{From, IsVariant, TryInto};

use crate::id::{ColId, LinkId, VarId};

use super::{CidValue, ColVal, Declaration, Polarity};

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum BodyTerm {
    Predicate(Predicate),
    Negation(Negation),
    GetLink(GetLink),
}

impl BodyTerm {
    pub fn vars(&self) -> HashSet<VarId> {
        match self {
            BodyTerm::Predicate(inner) => inner.vars(),
            BodyTerm::Negation(inner) => inner.vars(),
            BodyTerm::GetLink(inner) => inner.vars(),
        }
    }

    pub fn depends_on(&self) -> Vec<&Declaration> {
        match self {
            BodyTerm::Predicate(inner) => vec![inner.relation()],
            BodyTerm::Negation(inner) => vec![inner.relation()],
            BodyTerm::GetLink(_) => vec![],
        }
    }

    pub fn polarity(&self) -> Option<Polarity> {
        match self {
            BodyTerm::Predicate(_) => Some(Polarity::Positive),
            BodyTerm::Negation(_) => Some(Polarity::Negative),
            BodyTerm::GetLink(_) => Some(Polarity::Positive),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Predicate {
    relation: Arc<Declaration>,
    args: HashMap<ColId, ColVal>,
}

impl Predicate {
    pub fn new(relation: Arc<Declaration>, args: HashMap<ColId, ColVal>) -> Self {
        Self { relation, args }
    }

    pub fn relation(&self) -> &Arc<Declaration> {
        &self.relation
    }

    pub fn args(&self) -> &HashMap<ColId, ColVal> {
        &self.args
    }

    pub fn vars(&self) -> HashSet<VarId> {
        self.args
            .iter()
            .filter_map(|(_, v)| match v {
                ColVal::Lit(_) => None,
                ColVal::Binding(var) => Some(var.id()),
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

    pub fn relation(&self) -> &Arc<Declaration> {
        &self.relation
    }

    pub fn args(&self) -> &HashMap<ColId, ColVal> {
        &self.args
    }

    pub fn vars(&self) -> HashSet<VarId> {
        self.args
            .iter()
            .filter_map(|(_, v)| match v {
                ColVal::Lit(_) => None,
                ColVal::Binding(var) => Some(var.id()),
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

    // TODO: If we allowed link_id to be unbound we will need to add it here
    pub fn vars(&self) -> HashSet<VarId> {
        let mut vars = HashSet::default();

        if let CidValue::Var(var) = self.cid {
            vars.insert(var.id());
        }

        if let CidValue::Var(var) = self.link_value {
            vars.insert(var.id());
        }

        vars
    }
}

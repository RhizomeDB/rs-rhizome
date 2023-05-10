use std::collections::HashMap;

use crate::{
    col_val::ColVal,
    id::{ColId, RelationId},
};

use super::{BodyTerm, GetLink, Negation, Reduce, RelPredicate, VarPredicate};

#[derive(Debug)]
pub struct Rule {
    head: RelationId,
    args: HashMap<ColId, ColVal>,
    body: Vec<BodyTerm>,
}

impl Rule {
    pub fn new(head: RelationId, args: HashMap<ColId, ColVal>, body: Vec<BodyTerm>) -> Self {
        Self { head, args, body }
    }

    pub fn head(&self) -> RelationId {
        self.head
    }

    pub fn args(&self) -> &HashMap<ColId, ColVal> {
        &self.args
    }

    pub fn body(&self) -> &[BodyTerm] {
        &self.body
    }

    pub fn var_predicate_terms(&self) -> Vec<&VarPredicate> {
        self.body
            .iter()
            .filter_map(|term| {
                if let BodyTerm::VarPredicate(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn rel_predicate_terms(&self) -> Vec<&RelPredicate> {
        self.body
            .iter()
            .filter_map(|term| {
                if let BodyTerm::RelPredicate(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn negation_terms(&self) -> Vec<&Negation> {
        self.body
            .iter()
            .filter_map(|term| {
                if let BodyTerm::Negation(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_link_terms(&self) -> Vec<&GetLink> {
        self.body
            .iter()
            .filter_map(|term| {
                if let BodyTerm::GetLink(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn reduce_terms(&self) -> Vec<&Reduce> {
        self.body
            .iter()
            .filter_map(|term| {
                if let BodyTerm::Reduce(inner) = term {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect()
    }
}

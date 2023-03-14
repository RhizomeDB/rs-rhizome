use std::collections::HashMap;

use crate::{
    col_val::ColVal,
    id::{ColId, RelationId},
};

use super::{BodyTerm, Declaration, Edge, GetLink, Negation, Predicate};

#[derive(Debug, Clone, Eq, PartialEq)]
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

    pub fn body(&self) -> &Vec<BodyTerm> {
        &self.body
    }

    pub fn predicate_terms(&self) -> Vec<Predicate> {
        self.body_terms_of::<Predicate>()
    }

    pub fn negation_terms(&self) -> Vec<Negation> {
        self.body_terms_of::<Negation>()
    }

    pub fn get_link_terms(&self) -> Vec<GetLink> {
        self.body_terms_of::<GetLink>()
    }

    pub fn depends_on(&self) -> Vec<Edge> {
        let mut edges = Vec::default();

        for term in self.body() {
            if let Some(polarity) = term.polarity() {
                for dependency in term.depends_on() {
                    let edge = match dependency {
                        Declaration::Edb(inner) => Edge::FromEDB(inner.id(), self.head, polarity),
                        Declaration::Idb(inner) => Edge::FromIDB(inner.id(), self.head, polarity),
                    };

                    edges.push(edge);
                }
            }
        }

        edges
    }

    fn body_terms_of<T>(&self) -> Vec<T>
    where
        T: TryFrom<BodyTerm>,
    {
        self.body
            .iter()
            .filter_map(|term| T::try_from(term.clone()).ok())
            .collect()
    }
}

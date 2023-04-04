use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{fact::traits::IDBFact, id::RelationId, pretty::Pretty, relation::Relation};

#[derive(Debug)]
pub(crate) struct SinksBuilder<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
}

impl<F, R> Default for SinksBuilder<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    fn default() -> Self {
        Self {
            relations: HashMap::default(),
        }
    }
}

impl<F, R> SinksBuilder<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    pub(crate) fn add_relation(&mut self, id: RelationId, relation: Arc<RwLock<R>>) {
        self.relations.insert(id, relation);
    }

    pub(crate) fn finalize(self) -> Sinks<F, R> {
        Sinks {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Sinks<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
}

impl<F, R> Sinks<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    pub(crate) fn apply(&self, output: &mut VecDeque<F>) {
        for relation in self.relations.values() {
            for fact in relation.read().unwrap().iter() {
                output.push_back(fact.clone());
            }
        }
    }
}

impl<F, R> Pretty for Sinks<F, R>
where
    F: IDBFact,
    R: Relation<Fact = F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().into_iter().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sinks ").append(relations_doc)
    }
}

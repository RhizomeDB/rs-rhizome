use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{fact::traits::EDBFact, id::RelationId, pretty::Pretty, relation::Relation};

#[derive(Debug)]
pub(crate) struct SourcesBuilder<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
    _marker: PhantomData<F>,
}

impl<F, R> Default for SourcesBuilder<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    fn default() -> Self {
        Self {
            relations: HashMap::default(),
            _marker: PhantomData::default(),
        }
    }
}

impl<F, R> SourcesBuilder<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    pub(crate) fn add_relation(&mut self, id: RelationId, relation: Arc<RwLock<R>>) {
        self.relations.insert(id, relation);
    }

    pub(crate) fn finalize(self) -> Sources<F, R> {
        Sources {
            relations: self.relations,
            _marker: PhantomData::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Sources<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
    _marker: PhantomData<F>,
}

impl<F, R> Sources<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    pub(crate) fn apply(&self, input: &mut VecDeque<F>) -> bool {
        let mut has_new_facts = false;

        while let Some(fact) = input.pop_front() {
            let id = fact.id();
            let relation = self.relations.get(&id).unwrap();

            relation.write().unwrap().insert(fact);

            has_new_facts = true;
        }

        has_new_facts
    }
}

impl<F, R> Pretty for Sources<F, R>
where
    F: EDBFact,
    R: for<'a> Relation<'a, F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().into_iter().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sources ").append(relations_doc)
    }
}

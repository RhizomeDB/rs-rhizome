use anyhow::Result;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::EDBFact,
    id::RelationId,
    pretty::Pretty,
    relation::Relation,
};

#[derive(Debug)]
pub(crate) struct SourcesBuilder<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
}

impl<F, R> Default for SourcesBuilder<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    fn default() -> Self {
        Self {
            relations: HashMap::default(),
        }
    }
}

impl<F, R> SourcesBuilder<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    pub(crate) fn add_relation(&mut self, id: RelationId, relation: Arc<RwLock<R>>) {
        self.relations.insert(id, relation);
    }

    pub(crate) fn finalize(self) -> Sources<F, R> {
        Sources {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Sources<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    relations: HashMap<RelationId, Arc<RwLock<R>>>,
}

impl<F, R> Sources<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    pub(crate) fn apply(&self, input: &mut VecDeque<F>) -> Result<bool> {
        let mut has_new_facts = false;

        while let Some(fact) = input.pop_front() {
            let id = fact.id();
            let relation = self
                .relations
                .get(&id)
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?;

            relation
                .write()
                .or_else(|_| {
                    error(Error::InternalRhizomeError(
                        "relation lock poisoned".to_owned(),
                    ))
                })?
                .insert(fact);

            has_new_facts = true;
        }

        Ok(has_new_facts)
    }
}

impl<F, R> Pretty for Sources<F, R>
where
    F: EDBFact,
    R: Relation<Fact = F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().into_iter().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sources ").append(relations_doc)
    }
}

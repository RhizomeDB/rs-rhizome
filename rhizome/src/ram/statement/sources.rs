use anyhow::Result;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    id::{ColId, RelationId},
    pretty::Pretty,
    relation::Relation,
    tuple::Tuple,
    value::Val,
};

#[derive(Debug, Default)]
pub(crate) struct SourcesBuilder {
    relations: HashMap<RelationId, Arc<RwLock<Box<dyn Relation>>>>,
}

impl SourcesBuilder {
    pub(crate) fn add_relation(
        &mut self,
        id: RelationId,
        relation: Arc<RwLock<Box<dyn Relation>>>,
    ) {
        self.relations.insert(id, relation);
    }

    pub(crate) fn finalize(self) -> Sources {
        Sources {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Sources {
    relations: HashMap<RelationId, Arc<RwLock<Box<dyn Relation>>>>,
}

impl Sources {
    pub(crate) fn apply(&self, input: &mut VecDeque<Tuple>) -> Result<bool> {
        let mut has_new_facts = false;

        while let Some(fact) = input.pop_front() {
            let mut bindings: Vec<(ColId, Val)> = Vec::default();
            for col_id in fact.cols() {
                bindings.push((col_id, <Val>::clone(&fact.col(&col_id).unwrap())));
            }

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
                .insert(bindings, fact);

            has_new_facts = true;
        }

        Ok(has_new_facts)
    }
}

impl Pretty for Sources {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sources ").append(relations_doc)
    }
}

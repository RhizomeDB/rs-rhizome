use anyhow::Result;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    id::RelationId,
    pretty::Pretty,
    relation::Relation,
    tuple::Tuple,
};

#[derive(Debug, Default)]
pub(crate) struct SinksBuilder {
    pub(crate) relations: HashMap<RelationId, Arc<RwLock<Box<dyn Relation>>>>,
}

impl SinksBuilder {
    pub(crate) fn add_relation(
        &mut self,
        id: RelationId,
        relation: Arc<RwLock<Box<dyn Relation>>>,
    ) {
        self.relations.insert(id, relation);
    }

    pub(crate) fn finalize(self) -> Sinks {
        Sinks {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Sinks {
    relations: HashMap<RelationId, Arc<RwLock<Box<dyn Relation>>>>,
}

impl Sinks {
    pub(crate) fn apply(&self, output: &mut VecDeque<Tuple>) -> Result<()> {
        for relation in self.relations.values() {
            for fact in relation
                .read()
                .or_else(|_| {
                    error(Error::InternalRhizomeError(
                        "relation lock poisoned".to_owned(),
                    ))
                })?
                .search(vec![])
            {
                output.push_back(fact.clone());
            }
        }

        Ok(())
    }
}

impl Pretty for Sinks {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().map(RcDoc::as_string),
            RcDoc::text(", "),
        );

        RcDoc::text("sinks ").append(relations_doc)
    }
}

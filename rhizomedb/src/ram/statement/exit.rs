use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    pretty::Pretty,
    relation::{Relation, RelationKey},
};

#[derive(Debug, Default)]
pub(crate) struct ExitBuilder {
    relations: HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
}

impl ExitBuilder {
    pub(crate) fn add_relation(
        &mut self,
        relation_key: RelationKey,
        relation: Arc<RwLock<Box<dyn Relation>>>,
    ) {
        self.relations.insert(relation_key, relation);
    }

    pub(crate) fn finalize(self) -> Exit {
        Exit {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Exit {
    relations: HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
}

impl Exit {
    pub(crate) fn apply(&self) -> Result<bool> {
        for relation in self.relations.values() {
            let is_empty = relation
                .read()
                .or_else(|_| {
                    error(Error::InternalRhizomeError(
                        "relation lock poisoned".to_owned(),
                    ))
                })?
                .is_empty();

            if !is_empty {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl Pretty for Exit {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().map(|relation_key| {
                RcDoc::concat([
                    RcDoc::text("count("),
                    relation_key.to_doc(),
                    RcDoc::text(") == 0"),
                ])
            }),
            RcDoc::text(" or "),
        )
        .nest(1)
        .group();

        RcDoc::text("exit if ").append(relations_doc)
    }
}

use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::Fact,
    id::RelationId,
    pretty::Pretty,
    ram::RelationVersion,
    relation::Relation,
};

#[derive(Debug)]
pub(crate) struct ExitBuilder<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    relations: HashMap<(RelationId, RelationVersion), Arc<RwLock<R>>>,
}

impl<F, R> Default for ExitBuilder<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    fn default() -> Self {
        Self {
            relations: HashMap::default(),
        }
    }
}

impl<F, R> ExitBuilder<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    pub(crate) fn add_relation(
        &mut self,
        id: RelationId,
        version: RelationVersion,
        relation: Arc<RwLock<R>>,
    ) {
        self.relations.insert((id, version), relation);
    }

    pub(crate) fn finalize(self) -> Exit<F, R> {
        Exit {
            relations: self.relations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Exit<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    relations: HashMap<(RelationId, RelationVersion), Arc<RwLock<R>>>,
}

impl<F, R> Exit<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
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

impl<F, R> Pretty for Exit<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().map(|(id, version)| {
                RcDoc::concat([
                    RcDoc::text("count("),
                    RcDoc::as_string(id),
                    RcDoc::text("_"),
                    RcDoc::as_string(version),
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

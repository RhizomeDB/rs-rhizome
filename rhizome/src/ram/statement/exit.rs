use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    fact::traits::Fact, id::RelationId, pretty::Pretty, ram::RelationVersion, relation::Relation,
};

#[derive(Debug)]
pub(crate) struct ExitBuilder<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
{
    relations: HashMap<(RelationId, RelationVersion), Arc<RwLock<R>>>,
    _marker: PhantomData<F>,
}

impl<F, R> Default for ExitBuilder<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
{
    fn default() -> Self {
        Self {
            relations: HashMap::default(),
            _marker: PhantomData::default(),
        }
    }
}

impl<F, R> ExitBuilder<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
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
            _marker: PhantomData::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Exit<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
{
    relations: HashMap<(RelationId, RelationVersion), Arc<RwLock<R>>>,
    _marker: PhantomData<F>,
}

impl<F, R> Exit<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
{
    pub(crate) fn apply(&self) -> bool {
        self.relations
            .values()
            .all(|r| r.read().unwrap().is_empty())
    }
}

impl<F, R> Pretty for Exit<F, R>
where
    F: Fact,
    R: for<'a> Relation<'a, F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relations_doc = RcDoc::intersperse(
            self.relations.keys().into_iter().map(|(id, version)| {
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

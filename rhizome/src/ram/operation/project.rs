use anyhow::Result;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    pretty::Pretty,
    ram::{term::Term, Bindings, RelationVersion},
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
};

#[derive(Clone, Debug)]
pub(crate) struct Project<EF, IF, R>
where
    EF: EDBFact,
    IF: IDBFact,
    R: Relation<Fact = IF>,
{
    id: RelationId,
    version: RelationVersion,
    cols: HashMap<ColId, Term>,
    relation: Arc<RwLock<R>>,
    _marker: PhantomData<EF>,
}

impl<EF, IF, R> Project<EF, IF, R>
where
    EF: EDBFact,
    IF: IDBFact,
    R: Relation<Fact = IF>,
{
    pub(crate) fn new<A, T>(
        id: RelationId,
        version: RelationVersion,
        cols: impl IntoIterator<Item = (A, T)>,
        into: Arc<RwLock<R>>,
    ) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self {
            id,
            version,
            cols,
            relation: into,
            _marker: PhantomData,
        }
    }

    pub(crate) fn apply<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Result<()>
    where
        BS: Blockstore,
    {
        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in &self.cols {
            let val = bindings
                .resolve::<BS, EF>(term, blockstore)?
                .ok_or_else(|| {
                    Error::InternalRhizomeError("expected term to resolve".to_owned())
                })?;

            bound.push((*id, <Val>::clone(&val)));
        }

        let fact = IF::new(self.id, bound);

        self.relation
            .write()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .insert(fact);

        Ok(())
    }
}

impl<EF, IF, R> Pretty for Project<EF, IF, R>
where
    EF: EDBFact,
    IF: IDBFact,
    R: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols.iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(2)
        .group();

        RcDoc::concat([
            RcDoc::text("project "),
            RcDoc::text("("),
            cols_doc,
            RcDoc::text(")"),
            RcDoc::text(" into "),
            RcDoc::as_string(self.id),
            RcDoc::text("_"),
            RcDoc::as_string(self.version),
        ])
    }
}

use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    pretty::Pretty,
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
};

use super::{Bindings, RelationVersion, Term};

#[derive(Clone, Debug)]
pub(crate) enum NotInRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>),
}

#[derive(Clone, Debug)]
pub(crate) struct NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    id: RelationId,
    cols: HashMap<ColId, Term>,
    version: RelationVersion,
    relation: NotInRelation<EF, IF, ER, IR>,
}

impl<EF, IF, ER, IR> NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new<A, T>(
        id: RelationId,
        version: RelationVersion,
        cols: impl IntoIterator<Item = (A, T)>,
        relation: NotInRelation<EF, IF, ER, IR>,
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
            relation,
        }
    }

    pub(crate) fn cols(&self) -> &HashMap<ColId, Term> {
        &self.cols
    }

    pub(crate) fn is_satisfied<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Result<bool>
    where
        BS: Blockstore,
    {
        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in self.cols() {
            // TODO: Error if term fails to resolve; see https://github.com/RhizomeDB/rs-rhizome/issues/28
            if let Some(val) = bindings.resolve::<BS, EF>(term, blockstore)? {
                bound.push((*id, <Val>::clone(&val)));
            }
        }

        match &self.relation {
            NotInRelation::Edb(_) => {
                // See https://github.com/RhizomeDB/rs-rhizome/issues/27
                todo!("Negation is only implemented on IDB relations")
            }
            NotInRelation::Idb(relation) => {
                let bound_fact = IF::new(self.id, bound);

                Ok(!relation
                    .read()
                    .or_else(|_| {
                        error(Error::InternalRhizomeError(
                            "relation lock poisoned".to_owned(),
                        ))
                    })?
                    .contains(&bound_fact))
            }
        }
    }
}

impl<EF, IF, ER, IR> Pretty for NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols().iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(1)
        .group();

        RcDoc::concat([
            RcDoc::text("("),
            cols_doc,
            RcDoc::text(")"),
            RcDoc::text(" notin "),
            RcDoc::as_string(self.id),
            RcDoc::text("_"),
            RcDoc::as_string(self.version),
        ])
    }
}

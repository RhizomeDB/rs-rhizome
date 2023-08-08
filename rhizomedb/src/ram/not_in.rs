use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    id::ColId,
    pretty::Pretty,
    relation::{Relation, RelationKey},
    storage::blockstore::Blockstore,
    value::Val,
};

use super::{Bindings, Term};

#[derive(Clone, Debug)]
pub(crate) struct NotIn {
    relation_key: RelationKey,
    cols: HashMap<ColId, Term>,
    relation: Arc<RwLock<Box<dyn Relation>>>,
}

impl NotIn {
    pub(crate) fn new<A, T>(
        relation_key: RelationKey,
        cols: impl IntoIterator<Item = (A, T)>,
        relation: Arc<RwLock<Box<dyn Relation>>>,
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
            relation_key,
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
            if let Some(val) = bindings.resolve::<BS>(term, blockstore)? {
                bound.push((*id, <Val>::clone(&val)));
            } else {
                return error(Error::InternalRhizomeError(format!(
                    "failed to resolve term for column: {}",
                    id
                )));
            }
        }

        Ok(!self
            .relation
            .read()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .contains(bound))
    }
}

impl Pretty for NotIn {
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
            self.relation_key.to_doc(),
        ])
    }
}

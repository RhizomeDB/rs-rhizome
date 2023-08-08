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
    ram::{term::Term, Bindings, Formula},
    relation::{Relation, RelationKey},
    storage::blockstore::Blockstore,
    tuple::Tuple,
    value::Val,
};

#[derive(Clone, Debug)]
pub(crate) struct Project {
    relation_key: RelationKey,
    cols: HashMap<ColId, Term>,
    relation: Arc<RwLock<Box<dyn Relation>>>,
    formulae: Vec<Formula>,
}

impl Project {
    pub(crate) fn new<A, T>(
        relation_key: RelationKey,
        cols: impl IntoIterator<Item = (A, T)>,
        formulae: Vec<Formula>,
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
            formulae,
            relation,
        }
    }

    pub(crate) fn apply<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Result<()>
    where
        BS: Blockstore,
    {
        for formula in self.formulae.iter() {
            if !bindings.is_formula_satisfied::<BS>(formula, blockstore)? {
                return Ok(());
            }
        }

        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in &self.cols {
            if let Some(val) = bindings.resolve::<BS>(term, blockstore)? {
                bound.push((*id, <Val>::clone(&val)));
            } else {
                return Ok(());
            }
        }

        let fact = Tuple::new(self.relation_key.0, bound.clone(), None);

        self.relation
            .write()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .insert(bound, fact);

        Ok(())
    }
}

impl Pretty for Project {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols.iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(2)
        .group();

        let when_doc = if self.formulae.is_empty() {
            RcDoc::nil()
        } else {
            RcDoc::text(" where")
                .append(RcDoc::hardline())
                .append(RcDoc::text("("))
                .append(
                    RcDoc::intersperse(
                        self.formulae.iter().map(|formula| formula.to_doc()),
                        RcDoc::text(" and "),
                    )
                    .nest(1)
                    .group(),
                )
                .append(RcDoc::text(")"))
        };

        RcDoc::concat([
            RcDoc::text("project "),
            RcDoc::text("("),
            cols_doc,
            when_doc,
            RcDoc::text(")"),
            RcDoc::text(" into "),
            self.relation_key.to_doc(),
        ])
    }
}

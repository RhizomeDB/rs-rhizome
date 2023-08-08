use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    id::ColId,
    pretty::Pretty,
    ram::{alias_id::AliasId, formula::Formula, BindingKey, Bindings, Term},
    relation::{Relation, RelationKey},
    storage::blockstore::Blockstore,
    value::Val,
};

use super::Operation;

#[derive(Debug)]
pub(crate) struct Search {
    relation_key: RelationKey,
    alias: Option<AliasId>,
    relation: Arc<RwLock<Box<dyn Relation>>>,
    bindings: Vec<(ColId, Term)>,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl Search {
    pub(crate) fn new(
        relation_key: RelationKey,
        alias: Option<AliasId>,
        relation: Arc<RwLock<Box<dyn Relation>>>,
        bindings: Vec<(ColId, Term)>,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            relation_key,
            alias,
            relation,
            bindings,
            when,
            operation: Box::new(operation),
        }
    }

    pub(crate) fn operation(&self) -> &Operation {
        &self.operation
    }

    pub(crate) fn apply<BS, F>(&self, blockstore: &BS, bindings: &Bindings, f: F) -> Result<bool>
    where
        BS: Blockstore,
        F: Fn(Bindings) -> Result<bool>,
    {
        let mut bound_cols = vec![];
        for (col_id, term) in self.bindings.iter() {
            let resolved = bindings.resolve::<BS>(term, blockstore)?.ok_or_else(|| {
                Error::InternalRhizomeError("expected binding not found".to_owned())
            })?;

            bound_cols.push((*col_id, <Val>::clone(&resolved)));
        }

        for fact in self
            .relation
            .read()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .search(bound_cols)
        {
            let mut next_bindings = bindings.clone();

            // TODO: Only add the CID to the bindings if it's required by
            // a later operation.
            if let Some(cid) = fact.cid() {
                next_bindings.insert(
                    BindingKey::Cid(self.relation_key.0, self.alias),
                    Val::Cid(cid),
                );
            }

            for k in fact.cols() {
                let v = fact.col(&k).ok_or_else(|| {
                    Error::InternalRhizomeError("expected column not found".to_owned())
                })?;

                next_bindings.insert(
                    BindingKey::Relation(self.relation_key.0, self.alias, k),
                    v.clone(),
                );
            }

            let mut satisfied = true;
            for formula in self.when.iter() {
                if !next_bindings.is_formula_satisfied::<BS>(formula, blockstore)? {
                    satisfied = false;
                }
            }

            if !satisfied {
                continue;
            }

            if !f(next_bindings)? {
                return Ok(false);
            };
        }

        Ok(true)
    }
}

impl Pretty for Search {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relation_doc = match self.alias {
            Some(alias) => RcDoc::concat([
                self.relation_key.to_doc(),
                RcDoc::text(" as "),
                RcDoc::as_string(self.relation_key.0),
                RcDoc::text("_"),
                RcDoc::as_string(alias),
            ]),
            None => self.relation_key.to_doc(),
        };

        let when_doc = if self.when.is_empty() {
            RcDoc::nil()
        } else {
            RcDoc::text(" where")
                .append(RcDoc::hardline())
                .append(RcDoc::text("("))
                .append(
                    RcDoc::intersperse(
                        self.bindings
                            .iter()
                            .map(|(col_id, term)| {
                                RcDoc::concat([
                                    RcDoc::as_string(col_id),
                                    RcDoc::text(" = "),
                                    term.to_doc(),
                                ])
                            })
                            .chain(self.when.iter().map(|formula| formula.to_doc())),
                        RcDoc::text(" and "),
                    )
                    .nest(1)
                    .group(),
                )
                .append(RcDoc::text(")"))
        };

        RcDoc::concat([
            RcDoc::text("search "),
            relation_doc,
            when_doc,
            RcDoc::text(" do"),
        ])
        .append(
            RcDoc::hardline()
                .append(self.operation().to_doc())
                .nest(2)
                .group(),
        )
    }
}

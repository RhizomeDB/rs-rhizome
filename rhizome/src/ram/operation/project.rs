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
    ram::{term::Term, Bindings, Formula, RelationVersion},
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
};

#[derive(Clone, Debug)]
pub(crate) struct Project<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    id: RelationId,
    version: RelationVersion,
    cols: HashMap<ColId, Term>,
    relation: Arc<RwLock<IR>>,
    formulae: Vec<Formula<EF, IF, ER, IR>>,
    _marker: PhantomData<(EF, ER)>,
}

impl<EF, IF, ER, IR> Project<EF, IF, ER, IR>
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
        formulae: Vec<Formula<EF, IF, ER, IR>>,
        into: Arc<RwLock<IR>>,
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
            formulae,
            relation: into,
            _marker: PhantomData,
        }
    }

    pub(crate) fn apply<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Result<()>
    where
        BS: Blockstore,
    {
        for formula in self.formulae.iter() {
            if !bindings.is_formula_satisfied::<BS, EF, IF, ER, IR>(formula, blockstore)? {
                return Ok(());
            }
        }

        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in &self.cols {
            if let Some(val) = bindings.resolve::<BS, EF>(term, blockstore)? {
                bound.push((*id, <Val>::clone(&val)));
            } else {
                return Ok(());
            }
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

impl<EF, IF, ER, IR> Pretty for Project<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
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
            RcDoc::as_string(self.id),
            RcDoc::text("_"),
            RcDoc::as_string(self.version),
        ])
    }
}

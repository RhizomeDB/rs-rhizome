use anyhow::Result;
use std::{
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, Fact, IDBFact},
    id::RelationId,
    pretty::Pretty,
    ram::{alias_id::AliasId, formula::Formula, BindingKey, Bindings, RelationVersion},
    relation::Relation,
    storage::blockstore::Blockstore,
};

use super::Operation;

#[derive(Clone, Debug)]
pub(crate) enum SearchRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    Edb {
        relation: Arc<RwLock<ER>>,
        _marker: PhantomData<EF>,
    },
    Idb {
        relation: Arc<RwLock<IR>>,
        _marker: PhantomData<IF>,
    },
}

#[derive(Debug)]
pub(crate) struct Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    id: RelationId,
    alias: Option<AliasId>,
    version: RelationVersion,
    relation: SearchRelation<EF, IF, ER, IR>,
    when: Vec<Formula<EF, IF, ER, IR>>,
    operation: Box<Operation<EF, IF, ER, IR>>,
    _marker: PhantomData<(EF, IF, ER, IR)>,
}

impl<EF, IF, ER, IR> Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    pub(crate) fn new(
        id: RelationId,
        alias: Option<AliasId>,
        version: RelationVersion,
        relation: SearchRelation<EF, IF, ER, IR>,
        when: impl IntoIterator<Item = Formula<EF, IF, ER, IR>>,
        operation: Operation<EF, IF, ER, IR>,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            id,
            alias,
            version,
            relation,
            when,
            operation: Box::new(operation),
            _marker: PhantomData::default(),
        }
    }

    pub(crate) fn operation(&self) -> &Operation<EF, IF, ER, IR> {
        &self.operation
    }

    pub(crate) fn apply<BS, F>(&self, blockstore: &BS, bindings: &Bindings, f: F) -> Result<bool>
    where
        BS: Blockstore,
        F: Fn(Bindings) -> Result<bool>,
    {
        match &self.relation {
            SearchRelation::Edb { relation, .. } => {
                self.do_apply::<BS, EF, ER, F>(blockstore, bindings, relation, f)
            }
            SearchRelation::Idb { relation, .. } => {
                self.do_apply::<BS, IF, IR, F>(blockstore, bindings, relation, f)
            }
        }
    }

    fn do_apply<BS, F, R, WithBindings>(
        &self,
        blockstore: &BS,
        bindings: &Bindings,
        relation: &Arc<RwLock<R>>,
        f: WithBindings,
    ) -> Result<bool>
    where
        BS: Blockstore,
        F: Fact,
        R: for<'r> Relation<'r, F>,
        WithBindings: Fn(Bindings) -> Result<bool>,
    {
        for fact in relation.read().unwrap().iter() {
            let mut next_bindings = bindings.clone();

            for k in fact.cols() {
                if let Some(v) = fact.col(&k) {
                    next_bindings.insert(BindingKey::Relation(self.id, self.alias, k), v.clone());
                } else {
                    panic!("expected column missing: {k}");
                }
            }

            if !self
                .when
                .iter()
                .all(|f| next_bindings.is_formula_satisfied::<BS, EF, IF, ER, IR>(f, blockstore))
            {
                continue;
            }

            if !f(next_bindings)? {
                return Ok(false);
            };
        }

        Ok(true)
    }
}

impl<EF, IF, ER, IR> Pretty for Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relation_doc = match self.alias {
            Some(alias) => RcDoc::concat([
                RcDoc::as_string(self.id),
                RcDoc::text("_"),
                RcDoc::as_string(self.version),
                RcDoc::text(" as "),
                RcDoc::as_string(self.id),
                RcDoc::text("_"),
                RcDoc::as_string(alias),
            ]),
            None => RcDoc::concat([
                RcDoc::as_string(self.id),
                RcDoc::text("_"),
                RcDoc::as_string(self.version),
            ]),
        };

        let when_doc = if self.when.is_empty() {
            RcDoc::nil()
        } else {
            RcDoc::text(" where")
                .append(RcDoc::hardline())
                .append(RcDoc::text("("))
                .append(
                    RcDoc::intersperse(
                        self.when.iter().map(|formula| formula.to_doc()),
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
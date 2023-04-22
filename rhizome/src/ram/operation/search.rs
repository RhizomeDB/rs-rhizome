use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
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
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>),
}

#[derive(Debug)]
pub(crate) struct Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    id: RelationId,
    alias: Option<AliasId>,
    version: RelationVersion,
    relation: SearchRelation<EF, IF, ER, IR>,
    when: Vec<Formula<EF, IF, ER, IR>>,
    operation: Box<Operation<EF, IF, ER, IR>>,
}

impl<EF, IF, ER, IR> Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
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
            SearchRelation::Edb(relation) => {
                self.do_apply::<BS, EF, ER, F>(blockstore, bindings, relation, f)
            }
            SearchRelation::Idb(relation) => {
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
        R: Relation<Fact = F>,
        WithBindings: Fn(Bindings) -> Result<bool>,
    {
        for fact in relation
            .read()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .iter()
        {
            let mut next_bindings = bindings.clone();

            for k in fact.cols() {
                let v = fact.col(&k).ok_or_else(|| {
                    Error::InternalRhizomeError("expected column not found".to_owned())
                })?;

                next_bindings.insert(BindingKey::Relation(self.id, self.alias, k), v.clone());
            }

            let mut satisfied = true;
            for formula in self.when.iter() {
                if !next_bindings.is_formula_satisfied::<BS, EF, IF, ER, IR>(formula, blockstore)? {
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

impl<EF, IF, ER, IR> Pretty for Search<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
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

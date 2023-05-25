use anyhow::Result;
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    aggregation::AggregateWrapper,
    error::{error, Error},
    fact::traits::{EDBFact, Fact, IDBFact},
    id::{ColId, RelationId},
    pretty::Pretty,
    ram::{AliasId, BindingKey, Bindings, Formula, Term},
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
    var::Var,
};

use super::Operation;

#[derive(Clone, Debug)]
pub(crate) enum AggregationRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>),
}

pub(crate) struct Aggregation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    args: Vec<Term>,
    agg: Arc<dyn AggregateWrapper>,
    group_by_cols: HashMap<ColId, Term>,
    target: Var,
    id: RelationId,
    alias: Option<AliasId>,
    relation: AggregationRelation<EF, IF, ER, IR>,
    when: Vec<Formula<EF, IF, ER, IR>>,
    operation: Box<Operation<EF, IF, ER, IR>>,
}

impl<EF, IF, ER, IR> Aggregation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    // TODO: This struct is a mess and needs to be cleaned up.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        args: Vec<Term>,
        f: Arc<dyn AggregateWrapper>,
        target: Var,
        group_by_cols: HashMap<ColId, Term>,
        id: RelationId,
        alias: Option<AliasId>,
        relation: AggregationRelation<EF, IF, ER, IR>,
        when: impl IntoIterator<Item = Formula<EF, IF, ER, IR>>,
        operation: Operation<EF, IF, ER, IR>,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            args,
            agg: f,
            target,
            group_by_cols,
            id,
            alias,
            relation,
            when,
            operation: Box::new(operation),
        }
    }

    pub(crate) fn operation(&self) -> &Operation<EF, IF, ER, IR> {
        &self.operation
    }

    pub(crate) fn apply<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Result<Option<Bindings>>
    where
        BS: Blockstore,
    {
        match &self.relation {
            AggregationRelation::Edb(relation) => self.do_apply(blockstore, bindings, relation),
            AggregationRelation::Idb(relation) => self.do_apply(blockstore, bindings, relation),
        }
    }

    fn do_apply<BS, F, R>(
        &self,
        blockstore: &BS,
        bindings: &Bindings,
        relation: &Arc<RwLock<R>>,
    ) -> Result<Option<Bindings>>
    where
        BS: Blockstore,
        F: Fact,
        R: Relation<Fact = F>,
    {
        let mut group_by_vals: Vec<(ColId, Val)> = Vec::default();
        for (col_id, col_term) in &self.group_by_cols {
            let col_val = bindings
                .resolve::<BS, EF>(col_term, blockstore)?
                .ok_or_else(|| {
                    Error::InternalRhizomeError(format!(
                        "expected term to resolve for col: {}",
                        col_id
                    ))
                })?;

            group_by_vals.push((*col_id, <Val>::clone(&col_val)));
        }

        let relation = relation.read().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        let mut result = self.agg.init();
        for fact in relation.search(group_by_vals) {
            let mut match_bindings = bindings.clone();

            for k in fact.cols() {
                let fact_val = fact.col(&k).ok_or_else(|| {
                    Error::InternalRhizomeError("expected column not found".to_owned())
                })?;

                match_bindings.insert(
                    BindingKey::Relation(self.id, self.alias, k),
                    fact_val.clone(),
                );
            }

            let mut args = Vec::default();
            for term in self.args.iter() {
                let resolved = match_bindings
                    .resolve::<BS, EF>(term, blockstore)?
                    .ok_or_else(|| {
                        Error::InternalRhizomeError(
                            "argument to aggregation failed to resolve".to_owned(),
                        )
                    })?;

                args.push(resolved);
            }

            result.step(args);
        }

        if let Some(result) = result.finalize() {
            let mut next_bindings = bindings.clone();
            next_bindings.insert(BindingKey::Agg(self.id, self.alias, self.target), result);

            Ok(Some(next_bindings))
        } else {
            Ok(None)
        }
    }
}

impl<EF, IF, ER, IR> Debug for Aggregation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Aggregation")
            .field("args", &self.args)
            .field("group_by_cols", &self.group_by_cols)
            .field("target", &self.target)
            .field("id", &self.id)
            .field("alias", &self.alias)
            .field("when", &self.when)
            // .field("operation", &self.operation)
            .finish()
    }
}

impl<EF, IF, ER, IR> Pretty for Aggregation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        // TODO: pretty print aggregation; see https://github.com/RhizomeDB/rs-rhizome/issues/26
        RcDoc::concat([RcDoc::text("TODO AGGREGATION "), self.operation().to_doc()])
    }
}

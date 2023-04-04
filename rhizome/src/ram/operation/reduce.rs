use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, Fact, IDBFact},
    id::{ColId, RelationId},
    logic::ReduceClosure,
    pretty::Pretty,
    ram::{AliasId, BindingKey, Bindings, Formula, Term},
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
    var::Var,
};

use super::Operation;

#[derive(Clone, Debug)]
pub(crate) enum ReduceRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>),
}

pub(crate) struct Reduce<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    args: Vec<Term>,
    init: Val,
    f: Arc<dyn ReduceClosure>,
    group_by_cols: HashMap<ColId, Term>,
    target: Var,
    id: RelationId,
    alias: Option<AliasId>,
    relation: ReduceRelation<EF, IF, ER, IR>,
    when: Vec<Formula<EF, IF, ER, IR>>,
    operation: Box<Operation<EF, IF, ER, IR>>,
}

impl<EF, IF, ER, IR> Reduce<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new(
        args: Vec<Term>,
        init: Val,
        f: Arc<dyn ReduceClosure>,
        target: Var,
        group_by_cols: HashMap<ColId, Term>,
        id: RelationId,
        alias: Option<AliasId>,
        relation: ReduceRelation<EF, IF, ER, IR>,
        when: impl IntoIterator<Item = Formula<EF, IF, ER, IR>>,
        operation: Operation<EF, IF, ER, IR>,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            args,
            init,
            f,
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

    pub(crate) fn apply<BS>(&self, blockstore: &BS, bindings: &Bindings) -> Option<Bindings>
    where
        BS: Blockstore,
    {
        match &self.relation {
            ReduceRelation::Edb(relation) => self.do_apply(blockstore, bindings, relation),
            ReduceRelation::Idb(relation) => self.do_apply(blockstore, bindings, relation),
        }
    }

    fn do_apply<BS, F, R>(
        &self,
        blockstore: &BS,
        bindings: &Bindings,
        relation: &Arc<RwLock<R>>,
    ) -> Option<Bindings>
    where
        BS: Blockstore,
        F: Fact,
        R: Relation<Fact = F>,
    {
        let mut group_by_vals: HashMap<ColId, Arc<Val>> = HashMap::default();
        for (col_id, col_term) in &self.group_by_cols {
            if let Some(col_val) = bindings.resolve::<BS, EF>(col_term, blockstore) {
                group_by_vals.insert(*col_id, col_val);
            } else {
                panic!();
            };
        }

        let mut any = false;
        let mut result = self.init.clone();

        for fact in relation.read().unwrap().iter() {
            let mut matches = true;
            for (col_id, col_val) in &group_by_vals {
                if *col_val != fact.col(col_id).unwrap() {
                    matches = false;

                    break;
                }
            }

            if matches {
                any = true;

                let mut match_bindings = bindings.clone();

                for k in fact.cols() {
                    if let Some(v) = fact.col(&k) {
                        match_bindings
                            .insert(BindingKey::Relation(self.id, self.alias, k), v.clone());
                    } else {
                        panic!("expected column missing: {k}");
                    }
                }

                let args = self
                    .args
                    .iter()
                    .map(|t| match_bindings.resolve::<BS, EF>(t, blockstore).unwrap())
                    .map(|v| Arc::try_unwrap(v).unwrap_or_else(|arc| (*arc).clone()))
                    .collect::<Vec<_>>();

                result = self.do_reduce(result, args);
            }
        }

        if any {
            let mut next_bindings = bindings.clone();
            next_bindings.insert(
                BindingKey::Agg(self.id, self.alias, self.target),
                Arc::new(result),
            );

            Some(next_bindings)
        } else {
            None
        }
    }

    fn do_reduce(&self, acc: Val, args: Vec<Val>) -> Val {
        (self.f)(acc, args)
    }
}

impl<EF, IF, ER, IR> Debug for Reduce<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reduce")
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

impl<EF, IF, ER, IR> Pretty for Reduce<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        // TODO
        RcDoc::concat([RcDoc::text("TODO REDUCE "), self.operation().to_doc()])
    }
}

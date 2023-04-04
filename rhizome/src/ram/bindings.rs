use std::sync::Arc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    relation::Relation,
    storage::{blockstore::Blockstore, DefaultCodec},
    value::Val,
    var::Var,
};

use super::{AliasId, Formula, Term};

#[derive(Debug, Clone, Default)]
pub(crate) struct Bindings(im::HashMap<BindingKey, Arc<Val>>);

// TODO: Put Links in here as they're resolved,
// so that we can memoize their resolution
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum BindingKey {
    Relation(RelationId, Option<AliasId>, ColId),
    Agg(RelationId, Option<AliasId>, Var),
}

impl Bindings {
    pub(crate) fn insert(&mut self, key: BindingKey, term: Arc<Val>) {
        self.0.insert(key, term);
    }

    pub(crate) fn resolve<BS, EF>(&self, term: &Term, blockstore: &BS) -> Option<Arc<Val>>
    where
        BS: Blockstore,
        EF: EDBFact,
    {
        match term {
            Term::Link(link_id, cid_term) => {
                let Some(cid_val) = self.resolve::<BS, EF>(cid_term, blockstore) else {
                    panic!();
                };

                let Val::Cid(cid) = &*cid_val else {
                    panic!();
                };

                let Ok(Some(fact)) = blockstore.get_serializable::<DefaultCodec, EF>(cid) else {
                        return None;
                    };

                fact.link(*link_id)
            }

            Term::Col(relation_id, alias, col_id) => self
                .0
                .get(&BindingKey::Relation(*relation_id, *alias, *col_id))
                .map(Arc::clone),

            Term::Lit(val) => Some(val).map(Arc::clone),

            Term::Agg(relation_id, alias, var) => self
                .0
                .get(&BindingKey::Agg(*relation_id, *alias, *var))
                .map(Arc::clone),
        }
    }

    pub(crate) fn is_formula_satisfied<BS, EF, IF, ER, IR>(
        &self,
        formula: &Formula<EF, IF, ER, IR>,
        blockstore: &BS,
    ) -> bool
    where
        BS: Blockstore,
        EF: EDBFact,
        IF: IDBFact,
        ER: Relation<Fact = EF>,
        IR: Relation<Fact = IF>,
    {
        match formula {
            Formula::Equality(inner) => {
                let left = self.resolve::<BS, EF>(inner.left(), blockstore);
                let right = self.resolve::<BS, EF>(inner.right(), blockstore);

                left == right
            }
            Formula::NotIn(inner) => inner.is_satisfied(blockstore, self),
            Formula::Predicate(inner) => {
                let args = inner
                    .args()
                    .iter()
                    .map(|t| self.resolve::<BS, EF>(t, blockstore).unwrap())
                    .map(|v| Arc::try_unwrap(v).unwrap_or_else(|arc| (*arc).clone()))
                    .collect::<Vec<_>>();

                inner.is_satisfied(args)
            }
        }
    }
}

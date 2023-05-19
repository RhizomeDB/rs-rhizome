use anyhow::Result;
use std::sync::Arc;

use crate::{
    error::{error, Error},
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
// so that we can memoize their resolution; see https://github.com/RhizomeDB/rs-rhizome/issues/23
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum BindingKey {
    Relation(RelationId, Option<AliasId>, ColId),
    Cid(RelationId, Option<AliasId>),
    Agg(RelationId, Option<AliasId>, Var),
}

impl Bindings {
    pub(crate) fn insert(&mut self, key: BindingKey, term: Arc<Val>) {
        self.0.insert(key, term);
    }

    pub(crate) fn resolve<BS, EF>(&self, term: &Term, blockstore: &BS) -> Result<Option<Arc<Val>>>
    where
        BS: Blockstore,
        EF: EDBFact,
    {
        match term {
            Term::Link(link_id, cid_term) => {
                if let Some(cid_val) = self.resolve::<BS, EF>(cid_term, blockstore)? {
                    let Val::Cid(cid) = &*cid_val else {
                   return error(Error::InternalRhizomeError("expected term to resolve to CID".to_owned()));
                };

                    let Ok(Some(fact)) = blockstore.get_serializable::<DefaultCodec, EF>(cid) else {
                        return Ok(None);
                    };

                    Ok(fact.link(*link_id))
                } else {
                    Ok(None)
                }
            }

            Term::Col(relation_id, alias, col_id) => Ok(self
                .0
                .get(&BindingKey::Relation(*relation_id, *alias, *col_id))
                .map(Arc::clone)),

            Term::Cid(relation_id, alias) => Ok(self
                .0
                .get(&BindingKey::Cid(*relation_id, *alias))
                .map(Arc::clone)),

            Term::Lit(val) => Ok(Some(val).map(Arc::clone)),

            Term::Agg(relation_id, alias, var) => Ok(self
                .0
                .get(&BindingKey::Agg(*relation_id, *alias, *var))
                .map(Arc::clone)),
        }
    }

    pub(crate) fn is_formula_satisfied<BS, EF, IF, ER, IR>(
        &self,
        formula: &Formula<EF, IF, ER, IR>,
        blockstore: &BS,
    ) -> Result<bool>
    where
        BS: Blockstore,
        EF: EDBFact,
        IF: IDBFact,
        ER: Relation<Fact = EF>,
        IR: Relation<Fact = IF>,
    {
        match formula {
            Formula::Equality(inner) => {
                let left = self.resolve::<BS, EF>(inner.left(), blockstore)?;
                let right = self.resolve::<BS, EF>(inner.right(), blockstore)?;

                Ok(left == right)
            }
            Formula::NotIn(inner) => inner.is_satisfied(blockstore, self),
            Formula::Predicate(inner) => {
                let mut args = Vec::default();
                for term in inner.args() {
                    let resolved = self.resolve::<BS, EF>(term, blockstore)?.ok_or_else(|| {
                        Error::InternalRhizomeError(
                            "argument to predicate failed to resolve".to_owned(),
                        )
                    })?;

                    let inner_val = Arc::try_unwrap(resolved).unwrap_or_else(|arc| (*arc).clone());

                    args.push(inner_val);
                }

                inner.is_satisfied(args)
            }
        }
    }
}

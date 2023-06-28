use anyhow::Result;

use crate::{
    error::Error,
    id::{ColId, RelationId},
    storage::blockstore::Blockstore,
    value::Val,
    var::Var,
};

use super::{AliasId, Formula, Term};

#[derive(Debug, Clone, Default)]
pub(crate) struct Bindings(im::HashMap<BindingKey, Val>);

// TODO: Put Links in here as they're resolved,
// so that we can memoize their resolution; see https://github.com/RhizomeDB/rs-rhizome/issues/23
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum BindingKey {
    Relation(RelationId, Option<AliasId>, ColId),
    Cid(RelationId, Option<AliasId>),
    Agg(RelationId, Option<AliasId>, Var),
}

impl Bindings {
    pub(crate) fn insert(&mut self, key: BindingKey, term: Val) {
        self.0.insert(key, term);
    }

    pub(crate) fn resolve<BS>(&self, term: &Term, _blockstore: &BS) -> Result<Option<Val>>
    where
        BS: Blockstore,
    {
        match term {
            Term::Col(relation_id, alias, col_id) => Ok(self
                .0
                .get(&BindingKey::Relation(*relation_id, *alias, *col_id))
                .cloned()),

            Term::Cid(relation_id, alias) => {
                Ok(self.0.get(&BindingKey::Cid(*relation_id, *alias)).cloned())
            }

            Term::Lit(val) => Ok(Some(val).cloned()),

            Term::Agg(relation_id, alias, var) => Ok(self
                .0
                .get(&BindingKey::Agg(*relation_id, *alias, *var))
                .cloned()),
        }
    }

    pub(crate) fn is_formula_satisfied<BS>(
        &self,
        formula: &Formula,
        blockstore: &BS,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        match formula {
            Formula::Equality(inner) => {
                let left = self.resolve::<BS>(inner.left(), blockstore)?;
                let right = self.resolve::<BS>(inner.right(), blockstore)?;

                Ok(left == right)
            }
            Formula::NotIn(inner) => inner.is_satisfied(blockstore, self),
            Formula::Predicate(inner) => {
                let mut args = Vec::default();
                for term in inner.args() {
                    let resolved = self.resolve::<BS>(term, blockstore)?.ok_or_else(|| {
                        Error::InternalRhizomeError(
                            "argument to predicate failed to resolve".to_owned(),
                        )
                    })?;

                    args.push(resolved);
                }

                inner.is_satisfied(args)
            }
        }
    }
}

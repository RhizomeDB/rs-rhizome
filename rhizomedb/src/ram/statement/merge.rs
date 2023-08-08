use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    pretty::Pretty,
    relation::{Relation, RelationKey},
};

#[derive(Clone, Debug)]
pub(crate) struct Merge {
    from_key: RelationKey,
    into_key: RelationKey,
    from_relation: Arc<RwLock<Box<dyn Relation>>>,
    into_relation: Arc<RwLock<Box<dyn Relation>>>,
}

impl Merge {
    pub(crate) fn new(
        from_key: RelationKey,
        into_key: RelationKey,
        from_relation: Arc<RwLock<Box<dyn Relation>>>,
        into_relation: Arc<RwLock<Box<dyn Relation>>>,
    ) -> Self {
        Self {
            from_key,
            into_key,
            from_relation,
            into_relation,
        }
    }

    pub(crate) fn apply(&self) -> Result<()> {
        let mut merge_into = self.into_relation.write().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        let merge_from = self.from_relation.read().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        merge_into.merge(&**merge_from);

        debug_assert!(merge_into.len() >= merge_from.len());

        Ok(())
    }
}

impl Pretty for Merge {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("merge "),
            self.from_key.to_doc(),
            RcDoc::text(" into "),
            self.into_key.to_doc(),
        ])
    }
}

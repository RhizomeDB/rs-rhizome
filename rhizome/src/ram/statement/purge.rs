use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    pretty::Pretty,
    relation::{Relation, RelationKey},
};

#[derive(Clone, Debug)]
pub(crate) struct Purge {
    relation_key: RelationKey,
    relation: Arc<RwLock<Box<dyn Relation>>>,
}

impl Purge {
    pub(crate) fn new(relation_key: RelationKey, relation: Arc<RwLock<Box<dyn Relation>>>) -> Self {
        Self {
            relation_key,
            relation,
        }
    }

    pub(crate) fn apply(&self) -> Result<()> {
        self.relation
            .write()
            .or_else(|_| {
                error(Error::InternalRhizomeError(
                    "relation lock poisoned".to_owned(),
                ))
            })?
            .purge();

        Ok(())
    }
}

impl Pretty for Purge {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([RcDoc::text("purge "), self.relation_key.to_doc()])
    }
}

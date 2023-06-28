use std::{
    mem,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use pretty::RcDoc;

use crate::{
    error::{error, Error},
    pretty::Pretty,
    relation::{Relation, RelationKey},
};

#[derive(Clone, Debug)]
pub(crate) struct Swap {
    left_key: RelationKey,
    right_key: RelationKey,
    left: Arc<RwLock<Box<dyn Relation>>>,
    right: Arc<RwLock<Box<dyn Relation>>>,
}

impl Swap {
    pub(crate) fn new(
        left_key: RelationKey,
        right_key: RelationKey,
        left: Arc<RwLock<Box<dyn Relation>>>,
        right: Arc<RwLock<Box<dyn Relation>>>,
    ) -> Self {
        Self {
            left_key,
            right_key,
            left,
            right,
        }
    }

    pub(crate) fn apply(&self) -> Result<()> {
        let mut left = self.left.write().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        let mut right = self.right.write().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        mem::swap(&mut *left, &mut *right);

        Ok(())
    }
}

impl Pretty for Swap {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("swap "),
            self.left_key.to_doc(),
            RcDoc::text(" and "),
            self.right_key.to_doc(),
        ])
    }
}

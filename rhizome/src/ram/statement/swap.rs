use std::{
    mem,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use pretty::RcDoc;

use crate::{
    error::{error, Error},
    id::RelationId,
    pretty::Pretty,
    ram::RelationVersion,
};

#[derive(Clone, Debug)]
pub(crate) struct Swap<R> {
    left_id: RelationId,
    right_id: RelationId,
    left_version: RelationVersion,
    right_version: RelationVersion,
    left: Arc<RwLock<R>>,
    right: Arc<RwLock<R>>,
}

impl<R> Swap<R> {
    pub(crate) fn new(
        left_id: RelationId,
        left_version: RelationVersion,
        right_id: RelationId,
        right_version: RelationVersion,
        left: Arc<RwLock<R>>,
        right: Arc<RwLock<R>>,
    ) -> Self {
        Self {
            left_id,
            left_version,
            right_id,
            right_version,
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

impl<R> Pretty for Swap<R> {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("swap "),
            RcDoc::as_string(self.left_id),
            RcDoc::text("_"),
            RcDoc::as_string(self.left_version),
            RcDoc::text(" and "),
            RcDoc::as_string(self.right_id),
            RcDoc::text("_"),
            RcDoc::as_string(self.right_version),
        ])
    }
}

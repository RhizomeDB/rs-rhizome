use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::Fact,
    id::RelationId,
    pretty::Pretty,
    ram::RelationVersion,
    relation::Relation,
};

#[derive(Clone, Debug)]
pub(crate) struct Merge<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    from_id: RelationId,
    into_id: RelationId,
    from_version: RelationVersion,
    into_version: RelationVersion,
    merge_from: Arc<RwLock<R>>,
    merge_into: Arc<RwLock<R>>,
}

impl<F, R> Merge<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    pub(crate) fn new(
        from_id: RelationId,
        from_version: RelationVersion,
        into_id: RelationId,
        into_version: RelationVersion,
        from: Arc<RwLock<R>>,
        into: Arc<RwLock<R>>,
    ) -> Self {
        Self {
            from_id,
            into_id,
            from_version,
            into_version,
            merge_from: from,
            merge_into: into,
        }
    }

    pub(crate) fn apply(&self) -> Result<()> {
        let mut merge_into = self.merge_into.write().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        let merge_from = self.merge_from.read().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        merge_into.merge(&merge_from);

        debug_assert!(merge_into.len() >= merge_from.len());

        Ok(())
    }
}

impl<F, R> Pretty for Merge<F, R>
where
    F: Fact,
    R: Relation<Fact = F>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("merge "),
            RcDoc::as_string(self.from_id),
            RcDoc::text("_"),
            RcDoc::as_string(self.from_version),
            RcDoc::text(" into "),
            RcDoc::as_string(self.into_id),
            RcDoc::text("_"),
            RcDoc::as_string(self.into_version),
        ])
    }
}

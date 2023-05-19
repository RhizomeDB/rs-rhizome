use anyhow::Result;
use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    error::{error, Error},
    fact::traits::{EDBFact, Fact, IDBFact},
    id::RelationId,
    pretty::Pretty,
    ram::RelationVersion,
    relation::Relation,
};

#[derive(Clone, Debug)]
pub(crate) enum MergeRelations<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>, Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>, Arc<RwLock<IR>>),
}

impl<EF, IF, ER, IR> MergeRelations<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn apply(&self) -> Result<()> {
        match self {
            MergeRelations::Edb(from, into) => Self::do_apply(from, into),
            MergeRelations::Idb(from, into) => Self::do_apply(from, into),
        }
    }

    fn do_apply<F, R>(from: &Arc<RwLock<R>>, into: &Arc<RwLock<R>>) -> Result<()>
    where
        F: Fact,
        R: Relation<Fact = F>,
    {
        let mut merge_into = into.write().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        let merge_from = from.read().or_else(|_| {
            error(Error::InternalRhizomeError(
                "relation lock poisoned".to_owned(),
            ))
        })?;

        *merge_into = merge_into.merge(&merge_from);

        debug_assert!(merge_into.len() >= merge_from.len());

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Merge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    from_id: RelationId,
    into_id: RelationId,
    from_version: RelationVersion,
    into_version: RelationVersion,
    relations: MergeRelations<EF, IF, ER, IR>,
}

impl<EF, IF, ER, IR> Merge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new(
        from_id: RelationId,
        from_version: RelationVersion,
        into_id: RelationId,
        into_version: RelationVersion,
        relations: MergeRelations<EF, IF, ER, IR>,
    ) -> Self {
        Self {
            from_id,
            into_id,
            from_version,
            into_version,
            relations,
        }
    }

    pub(crate) fn apply(&self) -> Result<()> {
        self.relations.apply()?;

        Ok(())
    }
}

impl<EF, IF, ER, IR> Pretty for Merge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
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

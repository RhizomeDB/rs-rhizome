use std::sync::{Arc, RwLock};

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    id::RelationId,
    pretty::Pretty,
    ram::RelationVersion,
    relation::Relation,
};

#[derive(Clone, Debug)]
pub(crate) enum PurgeRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    Edb(Arc<RwLock<ER>>),
    Idb(Arc<RwLock<IR>>),
}

impl<EF, IF, ER, IR> PurgeRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn apply(&self) {
        match self {
            PurgeRelation::Edb(relation) => *relation.write().unwrap() = ER::default(),
            PurgeRelation::Idb(relation) => *relation.write().unwrap() = IR::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Purge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    id: RelationId,
    version: RelationVersion,
    relation: PurgeRelation<EF, IF, ER, IR>,
}

impl<EF, IF, ER, IR> Purge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    pub(crate) fn new(
        id: RelationId,
        version: RelationVersion,
        relation: PurgeRelation<EF, IF, ER, IR>,
    ) -> Self {
        Self {
            id,
            version,
            relation,
        }
    }

    pub(crate) fn apply(&self) {
        self.relation.apply();
    }
}

impl<EF, IF, ER, IR> Pretty for Purge<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("purge "),
            RcDoc::as_string(self.id),
            RcDoc::text("_"),
            RcDoc::as_string(self.version),
        ])
    }
}

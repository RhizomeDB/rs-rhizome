use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use pretty::RcDoc;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    pretty::Pretty,
    relation::Relation,
    storage::blockstore::Blockstore,
    value::Val,
};

use super::{Bindings, RelationVersion, Term};

#[derive(Clone, Debug)]
pub(crate) enum NotInRelation<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    Edb {
        #[allow(dead_code)]
        relation: Arc<RwLock<ER>>,
        _marker: PhantomData<EF>,
    },
    Idb {
        relation: Arc<RwLock<IR>>,
        _marker: PhantomData<IF>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    id: RelationId,
    cols: HashMap<ColId, Term>,
    version: RelationVersion,
    relation: NotInRelation<EF, IF, ER, IR>,
}

impl<EF, IF, ER, IR> NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    pub(crate) fn new<A, T>(
        id: RelationId,
        version: RelationVersion,
        cols: impl IntoIterator<Item = (A, T)>,
        relation: NotInRelation<EF, IF, ER, IR>,
    ) -> Self
    where
        A: Into<ColId>,
        T: Into<Term>,
    {
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self {
            id,
            version,
            cols,
            relation,
        }
    }

    pub(crate) fn cols(&self) -> &HashMap<ColId, Term> {
        &self.cols
    }

    pub(crate) fn is_satisfied<BS>(&self, blockstore: &BS, bindings: &Bindings) -> bool
    where
        BS: Blockstore,
    {
        // TODO: Dry up constructing a fact from BTreeMap<ColId, Term>
        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in self.cols() {
            if let Some(val) = bindings.resolve::<BS, EF>(term, blockstore) {
                bound.push((*id, <Val>::clone(&val)));
            }
        }

        match &self.relation {
            NotInRelation::Edb { .. } => {
                todo!("Oops, apparently negation is only implemented on IDB relations")
            }
            NotInRelation::Idb { relation, .. } => {
                let bound_fact = IF::new(self.id, bound);

                !relation.read().unwrap().contains(&bound_fact)
            }
        }
    }
}

impl<EF, IF, ER, IR> Pretty for NotIn<EF, IF, ER, IR>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: for<'a> Relation<'a, EF>,
    IR: for<'a> Relation<'a, IF>,
{
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let cols_doc = RcDoc::intersperse(
            self.cols().iter().map(|(col, term)| {
                RcDoc::concat([RcDoc::as_string(col), RcDoc::text(": "), term.to_doc()])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(1)
        .group();

        RcDoc::concat([
            RcDoc::text("("),
            cols_doc,
            RcDoc::text(")"),
            RcDoc::text(" notin "),
            RcDoc::as_string(self.id),
            RcDoc::text("_"),
            RcDoc::as_string(self.version),
        ])
    }
}

use crate::{
    id::{ColId, RelationId},
    pretty::Pretty,
    tuple::Tuple,
    value::Val,
};
use as_any::AsAny;
use dyn_clone::DynClone;
use pretty::RcDoc;
use std::{fmt::Debug, hash::Hash};

pub(crate) mod bistore;
pub(crate) mod hexastore;
pub(crate) mod immutable_ord_set;
pub(crate) mod ord_set;

pub use bistore::Bistore;
pub use hexastore::Hexastore;
pub use immutable_ord_set::ImmutableOrdSetRelation;
pub use ord_set::OrdSetRelation;

pub(crate) type DefaultRelation = OrdSetRelation;

pub(crate) type RelationKey = (RelationId, Version);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Source {
    Edb,
    Idb,
}

use std::fmt::{self, Display};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) enum Version {
    Total,
    Delta,
    New,
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::Total => f.write_str("total"),
            Version::Delta => f.write_str("delta"),
            Version::New => f.write_str("new"),
        }
    }
}

// TODO: Keep track of the timestamp a fact was derived at?
pub trait Relation: Debug + DynClone + Send + Sync + AsAny + 'static {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool;
    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Tuple> + '_>;

    fn purge(&mut self);
    fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: Tuple);
    fn merge(&mut self, rhs: &dyn Relation);
}

dyn_clone::clone_trait_object!(Relation);

impl Relation for Box<dyn Relation> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        (**self).contains(bindings)
    }

    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Tuple> + '_> {
        (**self).search(bindings)
    }

    fn purge(&mut self) {
        (**self).purge()
    }

    fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: Tuple) {
        (**self).insert(bindings, val)
    }

    fn merge(&mut self, rhs: &dyn Relation) {
        (**self).merge(rhs)
    }
}

impl Pretty for RelationKey {
    fn to_doc(&self) -> pretty::RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::as_string(self.0),
            RcDoc::text("_"),
            RcDoc::as_string(self.1),
        ])
    }
}

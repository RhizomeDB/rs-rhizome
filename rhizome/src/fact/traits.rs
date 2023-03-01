use cid::Cid;
use std::fmt::Debug;
use std::{collections::BTreeMap, fmt::Display};

use crate::marker::{SourceMarker, EDB, IDB};
use crate::storage::content_addressable::ContentAddressable;
use crate::{
    datum::Datum,
    id::{AttributeId, LinkId, RelationId},
};

// TODO: These traits are kind of a mess.
pub trait Fact: Clone + Ord + PartialOrd + Display + Debug + Send + Sync {
    type Marker: SourceMarker;

    fn id(&self) -> RelationId;
    fn attribute(&self, id: &AttributeId) -> Option<Datum>;
    fn attributes(&self) -> BTreeMap<AttributeId, Datum>;
}

pub trait EDBFact: Fact<Marker = EDB> + ContentAddressable {
    fn new<A: Into<AttributeId> + Ord, L: Into<LinkId>, D: Into<Datum>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
        links: impl IntoIterator<Item = (L, Cid)>,
    ) -> Self;

    fn link(&self, id: LinkId) -> Option<&Cid>;
}
pub trait IDBFact: Fact<Marker = IDB> {
    fn new<A: Into<AttributeId> + Ord, D: Into<Datum>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
    ) -> Self;
}

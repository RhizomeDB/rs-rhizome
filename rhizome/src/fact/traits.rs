use cid::Cid;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use crate::{
    id::{ColId, LinkId, RelationId},
    relation::{RelationSource, EDB, IDB},
    storage::content_addressable::ContentAddressable,
    value::Val,
};

// TODO: These traits are kind of a mess.
pub trait Fact: Clone + Ord + PartialOrd + Display + Debug + Send + Sync {
    type Marker: RelationSource;

    fn col(&self, id: &ColId) -> Option<Val>;
    fn cols(&self) -> BTreeMap<ColId, Val>;
}

pub trait EDBFact: Fact<Marker = EDB> + ContentAddressable {
    fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: Vec<(&str, Cid)>,
    ) -> Self;

    fn id(&self) -> RelationId;
    fn link(&self, id: LinkId) -> Option<&Cid>;
}
pub trait IDBFact: Fact<Marker = IDB> {
    fn new<A: Into<ColId> + Ord, D: Into<Val>>(
        id: impl Into<RelationId>,
        attr: impl IntoIterator<Item = (A, D)>,
    ) -> Self;

    fn id(&self) -> RelationId;
}

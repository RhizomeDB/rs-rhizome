use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use crate::{
    id::{ColId, LinkId, RelationId},
    relation::{Edb, Idb, RelationSource},
    storage::content_addressable::ContentAddressable,
    value::Val,
};

// TODO: These traits are kind of a mess.
pub trait Fact: Clone + Ord + PartialOrd + Display + Debug + Send + Sync {
    type Marker: RelationSource;

    fn col(&self, id: &ColId) -> Option<Val>;
    fn cols(&self) -> BTreeMap<ColId, Val>;
}

pub trait EDBFact: Fact<Marker = Edb> + ContentAddressable {
    fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: Vec<(&str, Val)>,
    ) -> Self;

    fn id(&self) -> RelationId;
    fn link(&self, id: LinkId) -> Option<&Val>;

    fn cid(&self) -> Val {
        Val::Cid(ContentAddressable::cid(self))
    }
}
pub trait IDBFact: Fact<Marker = Idb> {
    fn new<A: Into<ColId> + Ord, D: Into<Val>>(
        id: impl Into<RelationId>,
        attr: impl IntoIterator<Item = (A, D)>,
    ) -> Self;

    fn id(&self) -> RelationId;
}

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use cid::Cid;

use crate::{
    id::{ColId, LinkId, RelationId},
    relation::{Edb, Idb, RelationSource},
    storage::content_addressable::ContentAddressable,
    value::Val,
};

// TODO: These traits are kind of a mess.
pub trait Fact: Clone + Ord + PartialOrd + Display + Debug + Send + Sync {
    type Marker: RelationSource;

    fn col(&self, id: &ColId) -> Option<Arc<Val>>;
    fn cols(&self) -> Vec<ColId>;
}

pub trait EDBFact: Fact<Marker = Edb> + ContentAddressable {
    fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: Vec<(&str, Cid)>,
    ) -> Self;

    fn id(&self) -> RelationId;
    fn cid(&self) -> Cid;
    fn link(&self, id: LinkId) -> Option<Arc<Val>>;
}
pub trait IDBFact: Fact<Marker = Idb> {
    fn new<A: Into<ColId> + Ord, D: Into<Val>>(
        id: impl Into<RelationId>,
        attr: impl IntoIterator<Item = (A, D)>,
    ) -> Self;

    fn id(&self) -> RelationId;
}

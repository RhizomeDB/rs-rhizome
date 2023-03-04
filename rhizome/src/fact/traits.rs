use cid::Cid;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use crate::{
    id::{ColumnId, LinkId, RelationId},
    relation::{RelationSource, EDB, IDB},
    storage::content_addressable::ContentAddressable,
    value::Value,
};

// TODO: These traits are kind of a mess.
pub trait Fact: Clone + Ord + PartialOrd + Display + Debug + Send + Sync {
    type Marker: RelationSource;

    fn attribute(&self, id: &ColumnId) -> Option<Value>;
    fn attributes(&self) -> BTreeMap<ColumnId, Value>;
}

pub trait EDBFact: Fact<Marker = EDB> + ContentAddressable {
    fn new(
        entity: impl Into<Value>,
        attribute: impl Into<Value>,
        value: impl Into<Value>,
        links: impl IntoIterator<Item = (LinkId, Cid)>,
    ) -> Self;

    fn id(&self) -> RelationId;
    fn link(&self, id: LinkId) -> Option<&Cid>;
}
pub trait IDBFact: Fact<Marker = IDB> {
    fn new<A: Into<ColumnId> + Ord, D: Into<Value>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
    ) -> Self;

    fn id(&self) -> RelationId;
}

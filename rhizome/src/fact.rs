use std::collections::BTreeMap;

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
    timestamp::Timestamp,
};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd)]
pub struct Fact<T> {
    id: RelationId,
    timestamp: T,
    attributes: BTreeMap<AttributeId, Datum>,
}

impl<T: Timestamp> Fact<T> {
    pub fn new(id: RelationId, timestamp: T, attributes: Vec<(AttributeId, Datum)>) -> Self {
        let attributes = BTreeMap::from_iter(attributes);

        Self {
            id,
            timestamp,
            attributes,
        }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn attributes(&self) -> &BTreeMap<AttributeId, Datum> {
        &self.attributes
    }

    pub fn attribute(&self, id: &AttributeId) -> Option<&Datum> {
        self.attributes.get(id)
    }
}

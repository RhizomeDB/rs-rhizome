use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
    timestamp::Timestamp,
};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Fact<T: Timestamp> {
    name: RelationId,
    timestamp: T,
    attributes: BTreeMap<AttributeId, Datum>,
}

impl<T: Timestamp> Fact<T> {
    pub fn new(name: RelationId, timestamp: T, attributes: Vec<(AttributeId, Datum)>) -> Self {
        let attributes = BTreeMap::from_iter(attributes);

        Self {
            name,
            timestamp,
            attributes,
        }
    }

    pub fn attribute(&self, id: &AttributeId) -> Option<&Datum> {
        self.attributes.get(id)
    }
}

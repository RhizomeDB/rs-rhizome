use std::{collections::BTreeMap, fmt::Display};

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
    timestamp::{DefaultTimestamp, Timestamp},
};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd)]
pub struct Fact<T = DefaultTimestamp> {
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

    pub fn with_timestamp<TS: Timestamp>(&self, timestamp: TS) -> Fact<TS> {
        Fact::<TS> {
            id: self.id,
            timestamp,
            attributes: self.attributes.clone(),
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

impl<T> Display for Fact<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attributes = self
            .attributes
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "{}({})", self.id, attributes)
    }
}

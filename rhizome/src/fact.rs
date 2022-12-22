use im::HashMap;

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
    timestamp::Timestamp,
};

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct Fact<T: Timestamp> {
    name: RelationId,
    timestamp: T,
    attributes: HashMap<AttributeId, Datum>,
}

impl<T: Timestamp> Fact<T> {
    pub fn new(name: RelationId, timestamp: T, attributes: Vec<(AttributeId, Datum)>) -> Self {
        let attributes = HashMap::from_iter(attributes);

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

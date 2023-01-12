use std::{collections::BTreeMap, fmt::Display};

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd)]
pub struct Fact {
    id: RelationId,
    attributes: BTreeMap<AttributeId, Datum>,
}

impl Fact {
    pub fn new(id: RelationId, attributes: Vec<(AttributeId, Datum)>) -> Self {
        let attributes = BTreeMap::from_iter(attributes);

        Self { id, attributes }
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

impl Display for Fact {
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

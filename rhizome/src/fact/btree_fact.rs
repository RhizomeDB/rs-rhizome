use std::{collections::BTreeMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
    marker::IDB,
};

use super::traits::{Fact, IDBFact};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BTreeFact {
    id: RelationId,
    attributes: BTreeMap<AttributeId, Datum>,
}

impl IDBFact for BTreeFact {
    fn new<A: Into<AttributeId> + Ord, D: Into<Datum>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
    ) -> Self {
        let id = id.into();
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { id, attributes }
    }
}

impl Fact for BTreeFact {
    type Marker = IDB;

    fn id(&self) -> RelationId {
        self.id
    }

    fn attribute(&self, id: &AttributeId) -> Option<Datum> {
        self.attributes.get(id).cloned()
    }

    fn attributes(&self) -> BTreeMap<AttributeId, Datum> {
        self.attributes.clone()
    }
}

impl Display for BTreeFact {
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

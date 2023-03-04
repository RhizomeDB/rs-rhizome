use std::{collections::BTreeMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{
    id::{ColumnId, RelationId},
    relation::IDB,
    value::Value,
};

use super::traits::{Fact, IDBFact};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BTreeFact {
    id: RelationId,
    attributes: BTreeMap<ColumnId, Value>,
}

impl IDBFact for BTreeFact {
    fn new<A: Into<ColumnId> + Ord, D: Into<Value>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
    ) -> Self {
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self {
            id: id.into(),
            attributes,
        }
    }

    fn id(&self) -> RelationId {
        self.id
    }
}

impl Fact for BTreeFact {
    type Marker = IDB;

    fn attribute(&self, id: &ColumnId) -> Option<Value> {
        self.attributes.get(id).cloned()
    }

    fn attributes(&self) -> BTreeMap<ColumnId, Value> {
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

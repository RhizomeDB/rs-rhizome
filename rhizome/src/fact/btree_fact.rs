use std::{collections::BTreeMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{
    id::{ColId, RelationId},
    relation::IdbMarker,
    value::Val,
};

use super::traits::{Fact, IDBFact};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BTreeFact {
    id: RelationId,
    cols: BTreeMap<ColId, Val>,
}

impl IDBFact for BTreeFact {
    fn new<A: Into<ColId> + Ord, D: Into<Val>>(
        id: impl Into<RelationId>,
        cols: impl IntoIterator<Item = (A, D)>,
    ) -> Self {
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self {
            id: id.into(),
            cols,
        }
    }

    fn id(&self) -> RelationId {
        self.id
    }
}

impl Fact for BTreeFact {
    type Marker = IdbMarker;

    fn cid(&self) -> anyhow::Result<Option<cid::Cid>> {
        Ok(None)
    }

    fn col(&self, id: &ColId) -> Option<Val> {
        self.cols.get(id).cloned()
    }

    fn cols(&self) -> Vec<ColId> {
        self.cols.keys().copied().collect()
    }
}

impl Display for BTreeFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .cols
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "{}({})", self.id, cols)
    }
}

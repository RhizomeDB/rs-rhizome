use std::{collections::BTreeMap, fmt::Display};

use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::{
    id::{ColumnId, LinkId, RelationId},
    relation::EDB,
    storage::content_addressable::ContentAddressable,
    value::Value,
};

use super::traits::{EDBFact, Fact};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EVACFact {
    pub entity: Value,
    pub attribute: Value,
    pub value: Value,
    pub causal_links: BTreeMap<LinkId, Cid>,
}

impl EDBFact for EVACFact {
    fn new(
        entity: impl Into<Value>,
        attribute: impl Into<Value>,
        value: impl Into<Value>,
        links: Vec<(&str, Cid)>,
    ) -> Self {
        let entity = entity.into();
        let attribute = attribute.into();
        let value = value.into();

        let causal_links = links.into_iter().map(|(k, v)| (k.into(), v)).collect();

        Self {
            entity,
            attribute,
            value,
            causal_links,
        }
    }

    fn id(&self) -> RelationId {
        RelationId::new("evac")
    }

    fn link(&self, id: LinkId) -> Option<&Cid> {
        self.causal_links.get(&id)
    }
}

impl Fact for EVACFact {
    type Marker = EDB;

    fn attribute(&self, id: &ColumnId) -> Option<Value> {
        if *id == ColumnId::new("cid") {
            Some(Value::Cid(self.cid()))
        } else if *id == ColumnId::new("entity") {
            Some(self.entity.clone())
        } else if *id == ColumnId::new("attribute") {
            Some(self.attribute.clone())
        } else if *id == ColumnId::new("value") {
            Some(self.value.clone())
        } else {
            None
        }
    }

    fn attributes(&self) -> BTreeMap<ColumnId, Value> {
        BTreeMap::from_iter([
            (ColumnId::new("cid"), Value::Cid(self.cid())),
            (ColumnId::new("entity"), self.entity.clone()),
            (ColumnId::new("attribute"), self.attribute.clone()),
            (ColumnId::new("value"), self.value.clone()),
        ])
    }
}

impl Display for EVACFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attributes = self
            .attributes()
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<String>>()
            .join(", ");

        let links = self
            .causal_links
            .iter()
            .map(|(k, v)| format!("{k}: \"{v}\""))
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "evac({attributes}, links: [{links}])")
    }
}

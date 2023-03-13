use std::{collections::BTreeMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{
    id::{ColId, LinkId, RelationId},
    relation::EDB,
    value::Val,
};

use super::traits::{EDBFact, Fact};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EVACFact {
    pub entity: Val,
    pub attr: Val,
    pub val: Val,
    pub causal_links: BTreeMap<LinkId, Val>,
}

impl EDBFact for EVACFact {
    fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: Vec<(&str, Val)>,
    ) -> Self {
        let entity = entity.into();
        let attr = attr.into();
        let val = val.into();

        let causal_links = links
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();

        Self {
            entity,
            attr,
            val,
            causal_links,
        }
    }

    fn id(&self) -> RelationId {
        RelationId::new("evac")
    }

    fn link(&self, id: LinkId) -> Option<&Val> {
        self.causal_links.get(&id)
    }
}

impl Fact for EVACFact {
    type Marker = EDB;

    fn col(&self, id: &ColId) -> Option<Val> {
        if *id == ColId::new("cid") {
            Some(self.cid())
        } else if *id == ColId::new("entity") {
            Some(self.entity.clone())
        } else if *id == ColId::new("attribute") {
            Some(self.attr.clone())
        } else if *id == ColId::new("value") {
            Some(self.val.clone())
        } else {
            None
        }
    }

    fn cols(&self) -> BTreeMap<ColId, Val> {
        BTreeMap::from_iter([
            (ColId::new("cid"), self.cid()),
            (ColId::new("entity"), self.entity.clone()),
            (ColId::new("attribute"), self.attr.clone()),
            (ColId::new("value"), self.val.clone()),
        ])
    }
}

impl Display for EVACFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .cols()
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

        write!(f, "evac({cols}, links: [{links}])")
    }
}

use core::fmt;
use std::{collections::BTreeMap, fmt::Display};

use anyhow::Result;
use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::{
    id::{ColId, LinkId, RelationId},
    relation::EdbMarker,
    storage::content_addressable::ContentAddressable,
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
        links: Vec<(LinkId, Cid)>,
    ) -> Self {
        let entity = entity.into();
        let attr = attr.into();
        let val = val.into();

        let causal_links = links.into_iter().map(|(k, v)| (k, Val::Cid(v))).collect();

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

    fn link(&self, id: LinkId) -> Option<Val> {
        self.causal_links.get(&id).cloned()
    }
}

impl Fact for EVACFact {
    type Marker = EdbMarker;

    fn cid(&self) -> Result<Option<Cid>> {
        let cid = ContentAddressable::cid(self)?;

        Ok(Some(cid))
    }

    fn col(&self, id: &ColId) -> Option<Val> {
        if *id == ColId::new("entity") {
            Some(self.entity.clone())
        } else if *id == ColId::new("attribute") {
            Some(self.attr.clone())
        } else if *id == ColId::new("value") {
            Some(self.val.clone())
        } else {
            None
        }
    }

    fn cols(&self) -> Vec<ColId> {
        vec![
            ColId::new("entity"),
            ColId::new("attribute"),
            ColId::new("value"),
        ]
    }
}

impl Display for EVACFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut cols = Vec::default();
        for col in self.cols() {
            let val = self.col(&col).ok_or(fmt::Error)?;
            let col = format!("{col}: {val}");

            cols.push(col);
        }

        let cols = cols.join(", ");

        let links = self
            .causal_links
            .iter()
            .map(|(k, v)| format!("{k}: \"{v}\""))
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "evac({cols}, links: [{links}])")
    }
}

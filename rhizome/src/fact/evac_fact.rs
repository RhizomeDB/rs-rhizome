use std::{collections::BTreeMap, fmt::Display, sync::Arc};

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
    #[serde(skip_serializing)]
    pub cid: Option<Cid>,
    #[serde(skip_serializing)]
    pub cid_val: Option<Arc<Val>>,
    pub entity: Arc<Val>,
    pub attr: Arc<Val>,
    pub val: Arc<Val>,
    pub causal_links: BTreeMap<LinkId, Arc<Val>>,
}

impl EDBFact for EVACFact {
    fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: Vec<(&str, Cid)>,
    ) -> Self {
        let entity = Arc::new(entity.into());
        let attr = Arc::new(attr.into());
        let val = Arc::new(val.into());

        let causal_links = links
            .into_iter()
            .map(|(k, v)| (k.into(), Arc::new(Val::Cid(v))))
            .collect();

        let mut fact = Self {
            entity,
            attr,
            val,
            causal_links,
            cid: None,
            cid_val: None,
        };

        fact.cid = Some(ContentAddressable::cid(&fact));
        fact.cid_val = fact.cid.map(|c| Arc::new(Val::Cid(c)));

        fact
    }

    fn id(&self) -> RelationId {
        RelationId::new("evac")
    }

    fn cid(&self) -> Cid {
        self.cid.unwrap()
    }

    fn link(&self, id: LinkId) -> Option<Arc<Val>> {
        self.causal_links.get(&id).map(Arc::clone)
    }
}

impl Fact for EVACFact {
    type Marker = EdbMarker;

    fn col(&self, id: &ColId) -> Option<Arc<Val>> {
        let val = if *id == ColId::new("cid") {
            self.cid_val.as_ref()
        } else if *id == ColId::new("entity") {
            Some(&self.entity)
        } else if *id == ColId::new("attribute") {
            Some(&self.attr)
        } else if *id == ColId::new("value") {
            Some(&self.val)
        } else {
            None
        };

        val.map(Arc::clone)
    }

    fn cols(&self) -> Vec<ColId> {
        vec![
            ColId::new("cid"),
            ColId::new("entity"),
            ColId::new("attribute"),
            ColId::new("value"),
        ]
    }
}

impl Display for EVACFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .cols()
            .iter()
            .map(|k| format!("{k}: {}", self.col(k).unwrap()))
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

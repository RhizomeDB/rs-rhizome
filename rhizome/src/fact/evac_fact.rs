use std::{collections::BTreeMap, fmt::Display};

use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::{
    datum::Datum,
    id::{AttributeId, LinkId, RelationId},
    marker::EDB,
    storage::{content_addressable::ContentAddressable, DefaultCodec},
};

use super::traits::{EDBFact, Fact};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EVACFact {
    pub entity_id: Datum,
    pub attribute_id: AttributeId,
    pub attribute_value: Datum,
    pub causal_links: BTreeMap<LinkId, Cid>,
}

impl EDBFact for EVACFact {
    fn new<A: Into<AttributeId> + Ord, L: Into<LinkId>, D: Into<Datum>>(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = (A, D)>,
        links: impl IntoIterator<Item = (L, Cid)>,
    ) -> Self {
        let id = id.into();

        assert!(id == RelationId::new("evac"));

        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<BTreeMap<AttributeId, Datum>>();

        let entity_id = *attributes.get(&AttributeId::new("entity")).unwrap();

        let attribute_id = attributes
            .get(&AttributeId::new("attribute"))
            .and_then(|v| match v {
                Datum::Bool(_) => panic!(),
                Datum::Int(_) => panic!(),
                Datum::String(s) => Some(AttributeId::new(s.resolve())),
                Datum::Cid(_) => panic!(),
            })
            .unwrap();

        let attribute_value = *attributes.get(&AttributeId::new("value")).unwrap();

        let causal_links = links.into_iter().map(|(k, v)| (k.into(), v)).collect();

        Self {
            entity_id,
            attribute_id,
            attribute_value,
            causal_links,
        }
    }

    fn link(&self, id: LinkId) -> Option<&Cid> {
        self.causal_links.get(&id)
    }
}

impl Fact for EVACFact {
    type Marker = EDB;

    fn id(&self) -> RelationId {
        RelationId::new("evac")
    }

    fn attribute(&self, id: &AttributeId) -> Option<Datum> {
        if *id == AttributeId::new("cid") {
            Some(Datum::cid(self.cid(DefaultCodec::default())))
        } else if *id == AttributeId::new("entity") {
            Some(self.entity_id)
        } else if *id == AttributeId::new("attribute") {
            Some(Datum::string(self.attribute_id.resolve()))
        } else if *id == AttributeId::new("value") {
            Some(self.attribute_value)
        } else {
            None
        }
    }

    fn attributes(&self) -> BTreeMap<AttributeId, Datum> {
        BTreeMap::from_iter([
            (
                AttributeId::new("cid"),
                Datum::cid(self.cid(DefaultCodec::default())),
            ),
            (AttributeId::new("entity"), self.entity_id),
            (
                AttributeId::new("attribute"),
                Datum::string(self.attribute_id.resolve()),
            ),
            (AttributeId::new("value"), self.attribute_value),
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

        write!(f, "evac({}, links: [{}])", attributes, links)
    }
}

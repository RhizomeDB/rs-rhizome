use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display};

use cid::Cid;

use crate::{
    id::{ColId, RelationId},
    storage::content_addressable::ContentAddressable,
    value::Val,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InputTuple {
    entity: Val,
    attr: Val,
    val: Val,
    links: Vec<Cid>,
}

impl InputTuple {
    pub fn new(
        entity: impl Into<Val>,
        attr: impl Into<Val>,
        val: impl Into<Val>,
        links: impl IntoIterator<Item = Cid>,
    ) -> Self {
        let entity = entity.into();
        let attribute = attr.into();
        let value = val.into();
        let links = links.into_iter().collect();

        Self {
            entity,
            attr: attribute,
            val: value,
            links,
        }
    }

    pub fn entity(&self) -> Val {
        self.entity.clone()
    }

    pub fn attr(&self) -> Val {
        self.attr.clone()
    }

    pub fn val(&self) -> Val {
        self.val.clone()
    }

    pub fn cid(&self) -> Result<Cid> {
        ContentAddressable::cid(self)
    }

    pub fn links(&self) -> &[Cid] {
        &self.links
    }

    pub fn normalize_as_tuples(&self) -> Result<Vec<Tuple>> {
        let cid = self.cid()?;
        let mut tuples = Vec::default();

        tuples.push(Tuple::new(
            "evac",
            [
                ("entity", self.entity()),
                ("attribute", self.attr()),
                ("value", self.val()),
            ],
            Some(cid),
        ));

        for link in self.links() {
            tuples.push(Tuple::new("links", [("from", cid), ("to", *link)], None));
        }

        Ok(tuples)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Tuple {
    id: RelationId,
    cols: BTreeMap<ColId, Val>,
    cid: Option<Cid>,
}

impl Tuple {
    pub fn new<A: Into<ColId> + Ord, D: Into<Val>>(
        id: impl Into<RelationId>,
        cols: impl IntoIterator<Item = (A, D)>,
        cid: Option<Cid>,
    ) -> Self {
        let id = id.into();
        let cols = cols
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { id, cols, cid }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn col(&self, id: &ColId) -> Option<Val> {
        self.cols.get(id).cloned()
    }

    pub fn cols(&self) -> Vec<ColId> {
        self.cols.keys().copied().collect()
    }

    pub fn cid(&self) -> Option<Cid> {
        self.cid
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .cols
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<String>>()
            .join(", ");

        if let Some(cid) = self.cid {
            write!(f, "{}({}) (CID = {})", self.id, cols, cid)
        } else {
            write!(f, "{}({})", self.id, cols)
        }
    }
}

use rhizome::{datum::Datum as RhizomeDatum, fact::Fact as RhizomeFact, timestamp::Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::Datum;

#[wasm_bindgen]
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fact {
    id: String,
    attributes: BTreeMap<String, Datum>,
}

impl Fact {
    pub fn new(id: String, attributes: Vec<(String, Datum)>) -> Self {
        let attributes = BTreeMap::from_iter(attributes);

        Self { id, attributes }
    }
}

impl<T: Timestamp> From<RhizomeFact<T>> for Fact {
    fn from(f: RhizomeFact<T>) -> Self {
        let attributes: Vec<(String, Datum)> = f
            .attributes()
            .iter()
            .map(|(k, v)| (k.clone().resolve(), <RhizomeDatum as Into<Datum>>::into(*v)))
            .collect();

        Fact::new(f.id().resolve(), attributes)
    }
}

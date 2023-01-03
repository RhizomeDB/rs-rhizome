use rhizome::{datum::Datum, fact::Fact as RhizomeFact, timestamp::Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, Eq, PartialEq)]
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
            .map(|(k, v)| (k.clone().as_str().to_string(), *v))
            .collect();

        Fact::new(f.id().as_str().to_string(), attributes)
    }
}

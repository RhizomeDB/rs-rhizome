use js_sys::{Map, Reflect};
use rhizome::{
    fact::Fact as RhizomeFact,
    id::{AttributeId, RelationId},
};

use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_downcast::DowncastJS;

use crate::Datum;

#[wasm_bindgen]
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, DowncastJS)]
pub struct Fact(RhizomeFact);

#[wasm_bindgen]
impl Fact {
    #[wasm_bindgen(constructor)]
    pub fn new(id: String, attributes: &Map) -> Result<Fact, JsValue> {
        let mut attrs = Vec::new();
        for entry in attributes.entries() {
            let entry = entry?;
            let key = Reflect::get(&entry, &0.into())?;
            let value = Reflect::get(&entry, &1.into())?;
            let datum: Datum = serde_wasm_bindgen::from_value(value).unwrap();
            let attr = (AttributeId::new(key.as_string().unwrap()), datum.inner());

            attrs.push(attr);
        }

        let inner = RhizomeFact::new(RelationId::new(id), attrs);

        Ok(Self(inner))
    }

    pub fn id(&self) -> String {
        self.inner().id().resolve()
    }

    pub fn attributes(&self) -> Map {
        let attrs = Map::new();

        for (k, v) in self.inner().attributes() {
            let v: Datum = (*v).into();

            attrs.set(
                &JsValue::from(k.resolve()),
                &serde_wasm_bindgen::to_value(&v).unwrap(),
            );
        }

        attrs
    }
}

impl Fact {
    pub fn inner(&self) -> &RhizomeFact {
        &self.0
    }
}

impl From<RhizomeFact> for Fact {
    fn from(f: RhizomeFact) -> Self {
        Self(f)
    }
}

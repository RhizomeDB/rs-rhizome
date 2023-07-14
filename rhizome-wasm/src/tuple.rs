use std::rc::Rc;

use rhizome::tuple::Tuple;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_downcast::DowncastJS;

use crate::Cid;

#[wasm_bindgen]
#[derive(Debug, Clone, DowncastJS)]
pub struct InputTuple(rhizome::tuple::InputTuple);

#[wasm_bindgen]
#[derive(Debug, Clone, DowncastJS)]
pub struct OutputTuple(Rc<Tuple>);

impl InputTuple {
    pub fn into_inner(self) -> rhizome::tuple::InputTuple {
        self.0
    }
}

#[wasm_bindgen]
impl InputTuple {
    #[wasm_bindgen(constructor)]
    pub fn new(entity: &str, attribute: &str, value: JsValue, links_arr: &js_sys::Array) -> Self {
        let mut links = Vec::default();

        for link in links_arr.iter() {
            if let Ok(val) = serde_wasm_bindgen::from_value::<Cid>(link) {
                links.push(val.inner());
            } else {
                panic!("expected CID")
            }
        }

        if let Some(val) = value.as_bool() {
            Self(rhizome::tuple::InputTuple::new(
                entity, attribute, val, links,
            ))
        } else if let Some(val) = value.as_f64() {
            Self(rhizome::tuple::InputTuple::new(
                entity, attribute, val as i64, links,
            ))
        } else if let Some(val) = value.as_string() {
            Self(rhizome::tuple::InputTuple::new(
                entity,
                attribute,
                val.as_ref(),
                links,
            ))
        } else if let Ok(val) = serde_wasm_bindgen::from_value::<Cid>(value) {
            Self(rhizome::tuple::InputTuple::new(
                entity,
                attribute,
                val.inner(),
                links,
            ))
        } else {
            panic!("unknown type")
        }
    }
}

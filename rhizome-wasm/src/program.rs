use rhizome::logic::ast::Program as RhizomeProgram;
use std::rc::Rc;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

#[wasm_bindgen]
#[derive(Debug)]
pub struct Program(Rc<RhizomeProgram>);

#[wasm_bindgen]
impl Program {
    #[wasm_bindgen(constructor)]
    pub fn new(i: &str) -> Result<Program, JsValue> {
        rhizome::parse(i).map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |p| Ok(Self(Rc::new(p))),
        )
    }

    #[wasm_bindgen]
    pub fn pretty(&self) -> Result<JsValue, JsValue> {
        rhizome::pretty(&self.0).map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |p| Ok(JsValue::from_str(&p)),
        )
    }
}

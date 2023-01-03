use rhizome::logic::ast::Program as RhizomeProgram;
use std::{collections::BTreeSet, rc::Rc};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::fact::Fact;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Program(Rc<RhizomeProgram>);

#[wasm_bindgen]
impl Program {
    #[wasm_bindgen(constructor)]
    pub fn parse(i: &str) -> Result<Program, JsValue> {
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

    #[wasm_bindgen]
    pub fn run(&self, relation: &str) -> Result<JsValue, JsValue> {
        rhizome::run(&self.0, relation).map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |fs| {
                Ok(serde_wasm_bindgen::to_value(
                    &fs.into_iter().map(|f| f.into()).collect::<BTreeSet<Fact>>(),
                )
                .unwrap())
            },
        )
    }
}

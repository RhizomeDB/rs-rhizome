use std::cell::RefCell;
use std::cmp::max;
use std::rc::Rc;

use anyhow::Result;
use rhizome::types::Any;
use rhizome::var::TypedVar;
use rhizome::RuleVars;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_downcast::DowncastJS;

use crate::Cid;

#[wasm_bindgen]
#[derive(Debug, Clone, DowncastJS)]
pub struct ProgramBuilder {
    inner: Rc<RefCell<rhizome::ProgramBuilder>>,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Copy, DowncastJS)]
pub struct Var {
    idx: usize,
}

impl ProgramBuilder {
    pub fn new(p: rhizome::ProgramBuilder) -> Self {
        Self {
            inner: Rc::new(RefCell::new(p)),
        }
    }
    pub fn take(self) -> rhizome::ProgramBuilder {
        self.inner.take()
    }
}

#[wasm_bindgen]
impl ProgramBuilder {
    pub fn input(&self, id: &str, schema: &js_sys::Object) -> Result<(), JsValue> {
        self.inner
            .borrow_mut()
            .input(id, |input| {
                let keys = js_sys::Reflect::own_keys(schema).unwrap();

                keys.iter().fold(input, |acc, key| {
                    let col_key = key.as_string().unwrap();
                    let col_typ = js_sys::Reflect::get(schema, &key)
                        .unwrap()
                        .as_string()
                        .unwrap();

                    match col_typ.as_str() {
                        "bool" => acc.column::<bool>(col_key.as_ref()),
                        "int" => acc.column::<i64>(col_key.as_ref()),
                        "string" => acc.column::<&str>(col_key.as_ref()),
                        "cid" => acc.column::<cid::Cid>(col_key.as_ref()),
                        _ => panic!("unknown type"),
                    }
                })
            })
            .unwrap();

        Ok(())
    }

    pub fn output(&self, id: &str, schema: &js_sys::Object) -> Result<(), JsValue> {
        self.inner
            .borrow_mut()
            .output(id, |input| {
                let keys = js_sys::Reflect::own_keys(schema).unwrap();

                keys.iter().fold(input, |acc, key| {
                    let col_key = key.as_string().unwrap();
                    let col_typ = js_sys::Reflect::get(schema, &key)
                        .unwrap()
                        .as_string()
                        .unwrap();

                    match col_typ.as_str() {
                        "bool" => acc.column::<bool>(col_key.as_ref()),
                        "int" => acc.column::<i64>(col_key.as_ref()),
                        "string" => acc.column::<&str>(col_key.as_ref()),
                        "cid" => acc.column::<cid::Cid>(col_key.as_ref()),
                        _ => panic!("unknown type"),
                    }
                })
            })
            .unwrap();

        Ok(())
    }

    pub fn fact(&self, id: &str, columns: &js_sys::Object) -> Result<(), JsValue> {
        self.inner
            .borrow_mut()
            .fact(id, |fact| {
                let keys = js_sys::Reflect::own_keys(columns).unwrap();

                keys.iter().fold(fact, |acc, key| {
                    let col_key = key.as_string().unwrap();
                    let col_val = js_sys::Reflect::get(columns, &key).unwrap();

                    // TODO: There has to be a better way.
                    if let Some(val) = col_val.as_bool() {
                        acc.bind_one((col_key.as_ref(), val))
                    } else if let Some(val) = col_val.as_f64() {
                        acc.bind_one((col_key.as_ref(), val as i64))
                    } else if let Some(val) = col_val.as_string() {
                        acc.bind_one((col_key.as_ref(), val.as_ref()))
                    } else if let Ok(val) = serde_wasm_bindgen::from_value::<Cid>(col_val) {
                        acc.bind_one((col_key.as_ref(), val.0))
                    } else {
                        panic!("unknown type")
                    }
                })
            })
            .unwrap();

        Ok(())
    }

    pub fn rule(
        &self,
        id: &str,
        h: &js_sys::Function,
        b: &js_sys::Function,
    ) -> Result<(), JsValue> {
        let num_vars = max(h.length(), b.length());

        match num_vars {
            0 => self.do_rule::<[Any; 0]>(id, h, b),
            1 => self.do_rule::<[Any; 1]>(id, h, b),
            2 => self.do_rule::<[Any; 2]>(id, h, b),
            3 => self.do_rule::<[Any; 3]>(id, h, b),
            4 => self.do_rule::<[Any; 4]>(id, h, b),
            5 => self.do_rule::<[Any; 5]>(id, h, b),
            6 => self.do_rule::<[Any; 6]>(id, h, b),
            7 => self.do_rule::<[Any; 7]>(id, h, b),
            8 => self.do_rule::<[Any; 8]>(id, h, b),
            9 => self.do_rule::<[Any; 9]>(id, h, b),
            _ => {
                panic!("too many variables")
            }
        }
    }

    fn do_rule<A>(
        &self,
        id: &str,
        h_js: &js_sys::Function,
        b_js: &js_sys::Function,
    ) -> Result<(), JsValue>
    where
        A: RuleVars,
        A::Vars: IntoIterator<Item = TypedVar<Any>> + Clone,
    {
        self.inner
            .borrow_mut()
            .rule::<A>(id, &|h, b, vars| {
                let vars = Vec::from_iter(vars.clone().into_iter());
                let head_vars = vars
                    .iter()
                    .enumerate()
                    .map(|(i, _)| JsValue::from(Var { idx: i }))
                    .collect::<js_sys::Array>();

                let body_vars = vars
                    .iter()
                    .enumerate()
                    .map(|(i, _)| JsValue::from(Var { idx: i }))
                    .collect::<js_sys::Array>();

                let head_obj = h_js.apply(&JsValue::NULL, &head_vars).unwrap();
                let head_keys = js_sys::Reflect::own_keys(&head_obj).unwrap();

                head_keys.iter().for_each(|key| {
                    let col_key = key.as_string().unwrap();
                    let col_val = js_sys::Reflect::get(&head_obj, &key).unwrap();

                    if let Some(val) = col_val.as_bool() {
                        h.bind_one((col_key.as_ref(), val)).unwrap()
                    } else if let Some(val) = col_val.as_f64() {
                        h.bind_one((col_key.as_ref(), val as i64)).unwrap()
                    } else if let Some(val) = col_val.as_string() {
                        h.bind_one((col_key.as_ref(), val.as_str())).unwrap()
                    } else if let Ok(val) = serde_wasm_bindgen::from_value::<Cid>(col_val.clone()) {
                        h.bind_one((col_key.as_ref(), val.0)).unwrap()
                    } else if let Some(var) = Var::downcast_js_ref(&col_val) {
                        h.bind_one((col_key.as_ref(), vars.get(var.idx).unwrap()))
                            .unwrap()
                    } else {
                        panic!("unknown type")
                    }
                });

                let body_terms: js_sys::Array = b_js
                    .apply(&JsValue::NULL, &body_vars)
                    .unwrap()
                    .dyn_into()
                    .unwrap();

                body_terms.iter().for_each(|term| {
                    let op = js_sys::Reflect::get(&term, &JsValue::from("op"))
                        .unwrap()
                        .as_string()
                        .unwrap();

                    match op.as_ref() {
                        "search" => {
                            let id = js_sys::Reflect::get(&term, &JsValue::from("rel"))
                                .unwrap()
                                .as_string()
                                .unwrap();

                            let bindings =
                                js_sys::Reflect::get(&term, &JsValue::from("where")).unwrap();

                            let bindings_keys = js_sys::Reflect::own_keys(&bindings).unwrap();

                            b.build_search(id.as_ref(), |s| {
                                bindings_keys.iter().for_each(|key| {
                                    let col_key = key.as_string().unwrap();
                                    let col_val = js_sys::Reflect::get(&bindings, &key).unwrap();

                                    if let Some(val) = col_val.as_bool() {
                                        s.bind_one((col_key.as_ref(), val)).unwrap();
                                    } else if let Some(val) = col_val.as_f64() {
                                        s.bind_one((col_key.as_ref(), val as i64)).unwrap();
                                    } else if let Some(val) = col_val.as_string() {
                                        s.bind_one((col_key.as_ref(), val.as_str())).unwrap();
                                    } else if let Ok(val) =
                                        serde_wasm_bindgen::from_value::<Cid>(col_val.clone())
                                    {
                                        s.bind_one((col_key.as_ref(), val.0)).unwrap()
                                    } else if let Some(var) = Var::downcast_js_ref(&col_val) {
                                        s.bind_one((col_key.as_str(), vars.get(var.idx).unwrap()))
                                            .unwrap();
                                    } else {
                                        panic!("unknown type")
                                    }
                                });

                                Ok(())
                            })
                            .unwrap();
                        }
                        _ => panic!("unrecognized op"),
                    };
                });

                Ok(())
            })
            .unwrap();

        Ok(())
    }
}

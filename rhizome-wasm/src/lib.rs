#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

use futures::{sink::unfold, StreamExt};

use js_sys::AsyncIterator;
use rhizome::{tuple::Tuple, value::Val};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_downcast::DowncastJS;
use wasm_bindgen_futures::{spawn_local, stream::JsStream};

pub mod builder;
pub mod fact;

use std::{cell::RefCell, rc::Rc};

use crate::{builder::ProgramBuilder, fact::InputFact};

#[wasm_bindgen]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, DowncastJS)]
pub struct Cid(cid::Cid);

impl Cid {
    pub fn inner(&self) -> cid::Cid {
        self.0
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone, DowncastJS)]
pub struct Rhizome {
    client: Rc<RefCell<rhizome::runtime::client::Client>>,
    // event_rx: Rc<RefCell<Receiver<ClientEvent<PairTimestamp>>>>,
}

#[wasm_bindgen]
impl Rhizome {
    #[wasm_bindgen(constructor)]
    pub fn new(f: js_sys::Function) -> Self {
        let (client, mut rx, reactor) = rhizome::runtime::client::Client::new();

        spawn_local(async move {
            reactor
                .async_run(move |p| {
                    let builder = ProgramBuilder::new(p).unwrap();
                    let f_builder = builder.clone();

                    f.call1(&JsValue::NULL, &JsValue::from(f_builder)).unwrap();

                    Ok(builder.take())
                })
                .await
                .unwrap();
        });

        spawn_local(async move {
            loop {
                let _ = rx.next().await;
            }
        });

        Self {
            client: Rc::new(RefCell::new(client)),
            // event_rx: Rc::new(RefCell::new(rx)),
        }
    }

    pub async fn flush(&self) -> Result<(), JsValue> {
        self.client.borrow_mut().flush().await.map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |_| Ok(()),
        )
    }

    #[wasm_bindgen(js_name = registerStream)]
    pub async fn register_stream(
        &self,
        id: &str,
        async_iterator: AsyncIterator,
    ) -> Result<(), JsValue> {
        self.client
            .borrow_mut()
            .register_stream(
                id,
                Box::new(move || {
                    Box::new(JsStream::from(async_iterator).map(|fact| {
                        let fact = fact.unwrap();
                        let fact = InputFact::downcast_js_ref(&fact).unwrap();

                        fact.clone().into_inner()
                    }))
                }),
            )
            .await
            .map_or_else(
                |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
                |_| Ok(()),
            )
    }

    #[wasm_bindgen(js_name = registerSink)]
    pub async fn register_sink(&self, id: &str, f: js_sys::Function) -> Result<(), JsValue> {
        self.client
            .borrow_mut()
            .register_sink(
                id,
                Box::new(move || {
                    Box::new(unfold(f, move |f, fact: Tuple| async move {
                        let js_fact = js_sys::Object::new();

                        for col in fact.cols() {
                            match fact.col(&col).unwrap() {
                                Val::Bool(v) => js_sys::Reflect::set(
                                    &js_fact,
                                    &col.resolve().into(),
                                    &serde_wasm_bindgen::to_value(&v).unwrap(),
                                )
                                .unwrap(),
                                Val::S64(v) => js_sys::Reflect::set(
                                    &js_fact,
                                    &col.resolve().into(),
                                    &serde_wasm_bindgen::to_value(&v).unwrap(),
                                )
                                .unwrap(),
                                Val::String(v) => js_sys::Reflect::set(
                                    &js_fact,
                                    &col.resolve().into(),
                                    &serde_wasm_bindgen::to_value(&v).unwrap(),
                                )
                                .unwrap(),
                                Val::Cid(v) => js_sys::Reflect::set(
                                    &js_fact,
                                    &col.resolve().into(),
                                    &serde_wasm_bindgen::to_value(&Cid(v)).unwrap(),
                                )
                                .unwrap(),
                                _ => panic!("unsupported type"),
                            };
                        }

                        f.call1(&JsValue::NULL, &js_fact).unwrap();

                        Ok(f)
                    }))
                }),
            )
            .await
            .map_or_else(
                |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
                |_| Ok(()),
            )
    }
}

//------------------------------------------------------------------------------
// Utilities
//------------------------------------------------------------------------------

/// Panic hook lets us get better error messages if our Rust code ever panics.
///
/// For more details see
/// <https://github.com/rustwasm/console_error_panic_hook#readme>
#[wasm_bindgen(js_name = "setPanicHook")]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
extern "C" {
    // For alerting
    pub(crate) fn alert(s: &str);
    // For logging in the console.
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(s: &str);
}

//------------------------------------------------------------------------------
// Macros
//------------------------------------------------------------------------------

/// Return a representation of an object owned by JS.
#[macro_export]
macro_rules! value {
    ($value:expr) => {
        wasm_bindgen::JsValue::from($value)
    };
}

/// Calls the wasm_bindgen console.log.
#[macro_export]
macro_rules! console_log {
    ($($t:tt)*) => ($crate::log(&format_args!($($t)*).to_string()))
}

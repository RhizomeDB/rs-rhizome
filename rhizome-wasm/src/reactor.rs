use std::sync::Arc;

use futures::{lock::Mutex, sink::unfold, StreamExt};
use js_sys::AsyncIterator;
use rhizome::{fact::Fact as RhizomeFact, id::RelationId, reactor::Reactor as RhizomeReactor};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_downcast::DowncastJS;
use wasm_bindgen_futures::stream::JsStream;

use crate::{Fact, Receiver, Sender};

#[wasm_bindgen]
#[derive(Debug)]
pub struct Reactor(Arc<Mutex<RhizomeReactor>>);

#[wasm_bindgen]
impl Reactor {
    #[wasm_bindgen(constructor)]
    pub fn new(i: &str) -> Result<Reactor, JsValue> {
        rhizome::parse(i).map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |p| {
                rhizome::spawn(&p).map_or_else(
                    |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
                    |r| Ok(Self(Arc::new(Mutex::new(r)))),
                )
            },
        )
    }

    #[wasm_bindgen(js_name = inputChannel)]
    pub async fn input_channel(&self) -> Result<Sender, JsValue> {
        self.0.lock().await.input_channel().map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |ok| Ok(Sender::new(ok)),
        )
    }

    #[wasm_bindgen(js_name = outputChannel)]
    pub async fn output_channel(&self, relation_id: String) -> Result<Receiver, JsValue> {
        let relation_id = RelationId::new(relation_id);

        self.0.lock().await.output_channel(relation_id).map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            |ok| Ok(Receiver::new(ok)),
        )
    }

    #[wasm_bindgen(js_name = registerStream)]
    pub async fn register_stream(&self, async_iterator: AsyncIterator) -> Result<(), JsValue> {
        self.0
            .lock()
            .await
            .register_stream(move || {
                Box::new(JsStream::from(async_iterator).map(|maybe_fact| {
                    let fact = Fact::downcast_js_ref(&maybe_fact.unwrap()).unwrap();

                    fact.inner().clone()
                }))
            })
            .map_or_else(
                |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
                Ok,
            )
    }

    #[wasm_bindgen(js_name = registerSink)]
    pub async fn register_sink(
        &self,
        relation_id: String,
        callback: js_sys::Function,
    ) -> Result<(), JsValue> {
        let relation_id = RelationId::new(relation_id);

        self.0
            .lock()
            .await
            .register_sink(relation_id, move || {
                Box::new(unfold(
                    callback,
                    move |callback, fact: RhizomeFact| async move {
                        let f: Fact = fact.into();

                        callback.call1(&JsValue::null(), &JsValue::from(f)).unwrap();

                        Ok(callback)
                    },
                ))
            })
            .map_or_else(
                |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
                Ok,
            )
    }

    #[wasm_bindgen]
    pub async fn run(&self) -> Result<(), JsValue> {
        self.0.lock().await.async_run().await.map_or_else(
            |err: anyhow::Error| Err(serde_wasm_bindgen::to_value(&err.to_string())?),
            Ok,
        )
    }
}

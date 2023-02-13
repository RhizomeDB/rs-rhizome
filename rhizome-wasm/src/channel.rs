use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    stream::StreamExt,
    FutureExt,
};
use rhizome::fact::Fact as RhizomeFact;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_downcast::DowncastJS;

use crate::Fact;

#[wasm_bindgen]
#[derive(Debug, DowncastJS)]
pub struct Sender {
    channel: UnboundedSender<RhizomeFact>,
}

impl Sender {
    pub fn new(channel: UnboundedSender<RhizomeFact>) -> Self {
        Self { channel }
    }
}

#[wasm_bindgen]
impl Sender {
    #[wasm_bindgen]
    pub fn send(&self, maybe_fact: &JsValue) -> Result<(), JsValue> {
        let fact = Fact::downcast_js_ref(maybe_fact).unwrap().inner().clone();

        self.channel.unbounded_send(fact).unwrap();

        Ok(())
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.channel.close_channel();
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct Receiver {
    channel: UnboundedReceiver<RhizomeFact>,
}

impl Receiver {
    pub fn new(channel: UnboundedReceiver<RhizomeFact>) -> Self {
        Self { channel }
    }
}

// TODO: Are blocking output channels useful?
#[wasm_bindgen]
impl Receiver {
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<JsValue, JsValue> {
        self.channel
            .next()
            .map(|maybe_fact| {
                maybe_fact.map_or_else(
                    // TODO: Better error handling
                    || Err(serde_wasm_bindgen::to_value(&"Error".to_string()).unwrap()),
                    |f| {
                        let fact: Fact = f.into();

                        Ok(fact.into())
                    },
                )
            })
            .await
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.channel.close();
    }
}

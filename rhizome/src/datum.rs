use derive_more::{From, TryInto};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, From, Serialize, Deserialize, Eq, Hash, PartialEq, TryInto)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    // TODO: strings should be interned. Maybe using this:
    // https://docs.rs/string-interner/latest/string_interner/
    // Another good reference:
    // https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html
    String(String),
}

impl Datum {
    pub fn new(inner: impl Into<Self>) -> Self {
        inner.into()
    }
}

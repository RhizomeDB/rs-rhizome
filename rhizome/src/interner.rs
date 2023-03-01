use once_cell::sync::Lazy;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use std::{
    fmt::{self, Debug},
    sync::Mutex,
};
use string_interner::{DefaultSymbol, StringInterner};

static INSTANCE: Lazy<Mutex<StringInterner>> = Lazy::new(|| Mutex::new(StringInterner::default()));

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Symbol(DefaultSymbol);

impl Symbol {
    pub fn get_or_intern(s: &str) -> Symbol {
        Self(INSTANCE.lock().unwrap().get_or_intern(s))
    }

    pub fn resolve(&self) -> String {
        INSTANCE
            .lock()
            .unwrap()
            .resolve(self.0)
            .unwrap()
            .to_string()
    }
}

impl Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.resolve().as_str())
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.resolve().as_str())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(SymbolVisitor::default())
    }
}

#[derive(Default)]
struct SymbolVisitor {}

impl<'de> Visitor<'de> for SymbolVisitor {
    type Value = Symbol;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a str")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Symbol::get_or_intern(value))
    }
}

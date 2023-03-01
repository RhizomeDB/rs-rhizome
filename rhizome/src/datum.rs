use cid::Cid;
use derive_more::{From, TryInto};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::interner::Symbol;

#[derive(
    Debug, Clone, Copy, From, Eq, Hash, PartialEq, TryInto, Ord, PartialOrd, Serialize, Deserialize,
)]
pub enum Datum {
    Bool(bool),
    Int(i128),
    String(Symbol),
    Cid(Cid),
}

impl Datum {
    pub fn bool(data: bool) -> Self {
        Self::Bool(data)
    }

    pub fn int(data: i128) -> Self {
        Self::Int(data)
    }

    pub fn string<T: AsRef<str>>(data: T) -> Self {
        let symbol = Symbol::get_or_intern(data.as_ref());

        Self::String(symbol)
    }

    pub fn cid(data: Cid) -> Self {
        Self::Cid(data)
    }
}

impl From<&str> for Datum {
    fn from(value: &str) -> Self {
        Self::string(value)
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Datum::Bool(v) => v.to_string(),
            Datum::Int(v) => v.to_string(),
            Datum::String(v) => format!("{:?}", v.resolve()),
            Datum::Cid(v) => format!("{:?}", v.to_string()),
        };

        write!(f, "{}", s.as_str())
    }
}

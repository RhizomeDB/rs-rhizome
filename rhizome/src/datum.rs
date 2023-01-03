use std::fmt::Display;

use derive_more::{From, TryInto};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

#[derive(
    Debug, Clone, Copy, From, Eq, Hash, PartialEq, TryInto, Serialize, Deserialize, Ord, PartialOrd,
)]
#[serde(untagged)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    String(Ustr),
}

impl Datum {
    pub fn bool(data: bool) -> Self {
        Self::Bool(data)
    }

    pub fn int(data: i64) -> Self {
        Self::Int(data)
    }

    pub fn string<T: AsRef<str>>(data: T) -> Self {
        let symbol = Ustr::from(data.as_ref());

        Self::String(symbol)
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Datum::Bool(v) => v.to_string(),
            Datum::Int(v) => v.to_string(),
            Datum::String(v) => v.to_string(),
        };

        write!(f, "{}", s.as_str())
    }
}

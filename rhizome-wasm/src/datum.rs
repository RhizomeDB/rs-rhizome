use derive_more::{From, TryInto};
use rhizome::{datum::Datum as RhizomeDatum, interner};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(
    Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize, TryInto, From,
)]
#[serde(untagged)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    String(String),
}

impl Datum {
    pub fn bool(data: bool) -> Self {
        Self::Bool(data)
    }

    pub fn int(data: i64) -> Self {
        Self::Int(data)
    }

    pub fn string(data: String) -> Self {
        Self::String(data)
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

impl From<RhizomeDatum> for Datum {
    fn from(f: RhizomeDatum) -> Self {
        match f {
            RhizomeDatum::Bool(v) => Datum::bool(v),
            RhizomeDatum::Int(v) => Datum::int(v),
            RhizomeDatum::String(v) => Datum::string(interner::resolve(v)),
        }
    }
}

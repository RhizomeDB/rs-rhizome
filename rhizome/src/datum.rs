use std::fmt::Display;

use derive_more::{From, TryInto};

use crate::interner::{self, Symbol};

#[derive(Debug, Clone, Copy, From, Eq, Hash, PartialEq, TryInto, Ord, PartialOrd)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    String(Symbol),
}

impl Datum {
    pub fn bool(data: bool) -> Self {
        Self::Bool(data)
    }

    pub fn int(data: i64) -> Self {
        Self::Int(data)
    }

    pub fn string<T: AsRef<str>>(data: T) -> Self {
        let symbol = interner::get_or_intern(data.as_ref());

        Self::String(symbol)
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
            Datum::String(v) => format!("{:?}", interner::resolve(*v)),
        };

        write!(f, "{}", s.as_str())
    }
}

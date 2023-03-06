use std::fmt::{self, Display};

use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::types::Type;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    S8(i8),
    U8(u8),
    S16(i16),
    U16(u16),
    S32(i32),
    U32(u32),
    S64(i64),
    U64(u64),
    Char(char),
    String(Box<str>),
    Cid(Cid),
}

impl Value {
    pub fn type_of(&self) -> Type {
        match self {
            Value::Bool(_) => Type::Bool,
            Value::S8(_) => Type::S8,
            Value::U8(_) => Type::U8,
            Value::S16(_) => Type::S16,
            Value::U16(_) => Type::U16,
            Value::S32(_) => Type::S32,
            Value::U32(_) => Type::U32,
            Value::S64(_) => Type::S64,
            Value::U64(_) => Type::U64,
            Value::Char(_) => Type::Char,
            Value::String(_) => Type::String,
            Value::Cid(_) => Type::Cid,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Self::S8(value)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::S16(value)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::S32(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::S64(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<char> for Value {
    fn from(value: char) -> Self {
        Self::Char(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string().into_boxed_str())
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(v) => Display::fmt(v, f),
            Value::S8(v) => Display::fmt(v, f),
            Value::U8(v) => Display::fmt(v, f),
            Value::S16(v) => Display::fmt(v, f),
            Value::U16(v) => Display::fmt(v, f),
            Value::S32(v) => Display::fmt(v, f),
            Value::U32(v) => Display::fmt(v, f),
            Value::S64(v) => Display::fmt(v, f),
            Value::U64(v) => Display::fmt(v, f),
            Value::Char(v) => f.write_fmt(format_args!("{v:?}")),
            Value::String(v) => f.write_fmt(format_args!("{v:?}")),
            Value::Cid(v) => f.write_fmt(format_args!("\"{v}\"")),
        }
    }
}

use std::{
    fmt::{self, Display},
    sync::Arc,
};

use cid::Cid;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::types::Type;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Val {
    Bool(bool),
    S8(i8),
    U8(u8),
    S16(i16),
    U16(u16),
    S32(i32),
    U32(u32),
    F32(OrderedFloat<f32>),
    S64(i64),
    U64(u64),
    F64(OrderedFloat<f64>),
    Char(char),
    String(Arc<str>),
    Cid(Cid),
}

impl Val {
    pub fn type_of(&self) -> Type {
        match self {
            Val::Bool(_) => Type::Bool,
            Val::S8(_) => Type::S8,
            Val::U8(_) => Type::U8,
            Val::S16(_) => Type::S16,
            Val::U16(_) => Type::U16,
            Val::S32(_) => Type::S32,
            Val::U32(_) => Type::U32,
            Val::F32(_) => Type::F32,
            Val::S64(_) => Type::S64,
            Val::U64(_) => Type::U64,
            Val::F64(_) => Type::F64,
            Val::Char(_) => Type::Char,
            Val::String(_) => Type::String,
            Val::Cid(_) => Type::Cid,
        }
    }
}

impl From<bool> for Val {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i8> for Val {
    fn from(value: i8) -> Self {
        Self::S8(value)
    }
}

impl From<u8> for Val {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<i16> for Val {
    fn from(value: i16) -> Self {
        Self::S16(value)
    }
}

impl From<u16> for Val {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}

impl From<i32> for Val {
    fn from(value: i32) -> Self {
        Self::S32(value)
    }
}

impl From<u32> for Val {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<f32> for Val {
    fn from(value: f32) -> Self {
        Self::F32(value.into())
    }
}

impl From<i64> for Val {
    fn from(value: i64) -> Self {
        Self::S64(value)
    }
}

impl From<u64> for Val {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<f64> for Val {
    fn from(value: f64) -> Self {
        Self::F64(value.into())
    }
}

impl From<char> for Val {
    fn from(value: char) -> Self {
        Self::Char(value)
    }
}

impl From<&str> for Val {
    fn from(value: &str) -> Self {
        Self::String(Arc::from(value))
    }
}

impl From<String> for Val {
    fn from(value: String) -> Self {
        Self::String(Arc::from(value))
    }
}

impl From<Cid> for Val {
    fn from(value: Cid) -> Self {
        Self::Cid(value)
    }
}

impl TryFrom<Val> for bool {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::Bool(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for i8 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::S8(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for u8 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::U8(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for i16 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::S16(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for u16 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::U16(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for i32 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::S32(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for u32 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::U32(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for f32 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::F32(v) => Ok(*v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for i64 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::S64(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for u64 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::U64(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for f64 {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::F64(v) => Ok(*v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for char {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::Char(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for Arc<str> {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::String(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for String {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::String(v) => Ok(v.to_string()),
            _ => Err(()),
        }
    }
}

impl TryFrom<Val> for Cid {
    type Error = ();

    fn try_from(value: Val) -> Result<Self, Self::Error> {
        match value {
            Val::Cid(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl Display for Val {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Val::Bool(v) => Display::fmt(v, f),
            Val::S8(v) => Display::fmt(v, f),
            Val::U8(v) => Display::fmt(v, f),
            Val::S16(v) => Display::fmt(v, f),
            Val::U16(v) => Display::fmt(v, f),
            Val::S32(v) => Display::fmt(v, f),
            Val::U32(v) => Display::fmt(v, f),
            Val::F32(v) => Display::fmt(v, f),
            Val::S64(v) => Display::fmt(v, f),
            Val::U64(v) => Display::fmt(v, f),
            Val::F64(v) => Display::fmt(v, f),
            Val::Char(v) => f.write_fmt(format_args!("{v:?}")),
            Val::String(v) => f.write_fmt(format_args!("{v:?}")),
            Val::Cid(v) => f.write_fmt(format_args!("\"{v}\"")),
        }
    }
}

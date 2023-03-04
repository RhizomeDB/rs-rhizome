use std::{
    fmt::{self, Display},
    mem,
};

use crate::{
    error::{error, Error},
    value::Value,
};
use anyhow::Result;
use cid::Cid;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum ColumnType {
    #[default]
    Any,
    Type(Type),
}

impl ColumnType {
    pub fn new<T>() -> Self
    where
        Type: FromType<T>,
    {
        let t = FromType::<T>::from_type();

        Self::Type(t)
    }

    pub fn check(&self, value: &Value) -> Result<()> {
        match self {
            ColumnType::Any => Ok(()),
            ColumnType::Type(t) => t.check(value),
        }
    }

    #[allow(dead_code)]
    fn inner(&self) -> Option<&Type> {
        match self {
            ColumnType::Any => None,
            ColumnType::Type(t) => Some(t),
        }
    }
}

pub trait FromType<T> {
    fn from_type() -> Self;
}

impl FromType<bool> for Type {
    fn from_type() -> Self {
        Self::Bool
    }
}

impl FromType<i8> for Type {
    fn from_type() -> Self {
        Self::S8
    }
}

impl FromType<u8> for Type {
    fn from_type() -> Self {
        Self::U8
    }
}

impl FromType<i16> for Type {
    fn from_type() -> Self {
        Self::S16
    }
}

impl FromType<u16> for Type {
    fn from_type() -> Self {
        Self::U16
    }
}

impl FromType<i32> for Type {
    fn from_type() -> Self {
        Self::S32
    }
}

impl FromType<u32> for Type {
    fn from_type() -> Self {
        Self::U32
    }
}

impl FromType<i64> for Type {
    fn from_type() -> Self {
        Self::S64
    }
}

impl FromType<u64> for Type {
    fn from_type() -> Self {
        Self::U64
    }
}

impl FromType<char> for Type {
    fn from_type() -> Self {
        Self::Char
    }
}

impl FromType<&str> for Type {
    fn from_type() -> Self {
        Self::String
    }
}

impl FromType<Cid> for Type {
    fn from_type() -> Self {
        Self::Cid
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::Any => f.write_str("any"),
            ColumnType::Type(t) => Display::fmt(t, f),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Type {
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    Char,
    String,
    Cid,
}

impl Type {
    pub fn new<T>() -> Self
    where
        Self: FromType<T>,
    {
        FromType::<T>::from_type()
    }

    pub fn check(&self, value: &Value) -> Result<()> {
        let other = &value.type_of();

        if self == other {
            Ok(())
        } else if mem::discriminant(self) != mem::discriminant(other) {
            return error(Error::TypeMismatch(*self, *other));
        } else {
            unreachable!()
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::Bool => "bool",
            Type::S8 => "s8",
            Type::U8 => "u8",
            Type::S16 => "s16",
            Type::U16 => "u16",
            Type::S32 => "s32",
            Type::U32 => "u32",
            Type::S64 => "s64",
            Type::U64 => "u64",
            Type::Char => "char",
            Type::String => "string",
            Type::Cid => "CID",
        };

        f.write_str(s)
    }
}

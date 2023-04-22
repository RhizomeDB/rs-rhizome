use std::{
    fmt::{self, Display},
    mem,
};

use crate::{
    error::{error, Error},
    value::Val,
};
use anyhow::Result;
use cid::Cid;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum ColType {
    #[default]
    Any,
    Type(Type),
}

impl ColType {
    pub fn new<T>() -> Self
    where
        ColType: FromType<T>,
    {
        FromType::<T>::from_type()
    }

    pub fn downcast(&self, downcast_to: &Type) -> Option<Type> {
        match self {
            ColType::Any => Some(*downcast_to),
            ColType::Type(typ) if typ == downcast_to => Some(*downcast_to),
            _ => None,
        }
    }

    pub fn unify(&self, other: &ColType) -> Result<ColType> {
        match (self, other) {
            (ColType::Any, ColType::Any) => Ok(ColType::Any),
            (ColType::Any, ColType::Type(t)) => Ok(ColType::Type(*t)),
            (ColType::Type(t), ColType::Any) => Ok(ColType::Type(*t)),
            (ColType::Type(t1), ColType::Type(t2)) => t1.unify(t2).map(ColType::Type),
        }
    }

    pub fn check(&self, value: &Val) -> Result<()> {
        match self {
            ColType::Any => Ok(()),
            ColType::Type(t) => t.check(value),
        }
    }

    #[allow(dead_code)]
    fn inner(&self) -> Option<&Type> {
        match self {
            ColType::Any => None,
            ColType::Type(t) => Some(t),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Any {}

pub trait FromType<T> {
    fn from_type() -> Self;
}

impl<T> FromType<T> for ColType
where
    Type: FromType<T>,
{
    fn from_type() -> Self {
        let t = FromType::<T>::from_type();

        Self::Type(t)
    }
}

impl FromType<Any> for ColType {
    fn from_type() -> Self {
        Self::Any
    }
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

impl Display for ColType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColType::Any => f.write_str("any"),
            ColType::Type(t) => Display::fmt(t, f),
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
    Js,
}

impl Type {
    pub fn new<T>() -> Self
    where
        Self: FromType<T>,
    {
        FromType::<T>::from_type()
    }

    pub fn check(&self, value: &Val) -> Result<()> {
        let other = &value.type_of();

        self.unify(other).and(Ok(()))
    }

    pub fn unify(&self, other: &Type) -> Result<Type> {
        if self == other {
            Ok(*self)
        } else if mem::discriminant(self) != mem::discriminant(other) {
            error(Error::TypeMismatch(*self, *other))
        } else {
            error(Error::InternalRhizomeError(
                "unreachable case in type checking".to_owned(),
            ))
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
            Type::Js => "Js",
        };

        f.write_str(s)
    }
}

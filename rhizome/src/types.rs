use std::{
    fmt::{self, Display},
    mem,
    sync::Arc,
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
        T: IntoColType,
    {
        T::into_col_type()
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

pub trait IntoColType {
    fn into_col_type() -> ColType;
}

impl IntoColType for Any {
    fn into_col_type() -> ColType {
        ColType::Any
    }
}

impl IntoColType for bool {
    fn into_col_type() -> ColType {
        ColType::Type(Type::Bool)
    }
}

impl IntoColType for i8 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::S8)
    }
}

impl IntoColType for u8 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::U8)
    }
}

impl IntoColType for i16 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::S16)
    }
}

impl IntoColType for u16 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::U16)
    }
}

impl IntoColType for i32 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::S32)
    }
}

impl IntoColType for u32 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::U32)
    }
}

impl IntoColType for i64 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::S64)
    }
}

impl IntoColType for u64 {
    fn into_col_type() -> ColType {
        ColType::Type(Type::U64)
    }
}

impl IntoColType for char {
    fn into_col_type() -> ColType {
        ColType::Type(Type::Char)
    }
}

impl IntoColType for &str {
    fn into_col_type() -> ColType {
        ColType::Type(Type::String)
    }
}

impl IntoColType for Arc<str> {
    fn into_col_type() -> ColType {
        ColType::Type(Type::String)
    }
}

impl IntoColType for Cid {
    fn into_col_type() -> ColType {
        ColType::Type(Type::Cid)
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
}

impl Type {
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
        };

        f.write_str(s)
    }
}

pub trait RhizomeType: Clone + IntoColType + 'static {}

impl RhizomeType for bool {}
impl RhizomeType for i8 {}
impl RhizomeType for u8 {}
impl RhizomeType for i16 {}
impl RhizomeType for u16 {}
impl RhizomeType for i32 {}
impl RhizomeType for u32 {}
impl RhizomeType for i64 {}
impl RhizomeType for u64 {}
impl RhizomeType for char {}
impl RhizomeType for Arc<str> {}
impl RhizomeType for Cid {}

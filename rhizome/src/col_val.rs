use std::{
    fmt::{self, Display},
    sync::Arc,
};

use crate::{
    types::{ColType, FromType},
    value::Val,
    var::{TypedVar, Var},
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ColVal {
    Lit(Arc<Val>),
    Binding(Var),
}

impl From<Val> for ColVal {
    fn from(value: Val) -> Self {
        Self::Lit(Arc::new(value))
    }
}

impl From<&Val> for ColVal {
    fn from(value: &Val) -> Self {
        Self::Lit(Arc::new(value.clone()))
    }
}

impl From<Var> for ColVal {
    fn from(value: Var) -> Self {
        Self::Binding(value)
    }
}

impl From<&Var> for ColVal {
    fn from(value: &Var) -> Self {
        Self::Binding(*value)
    }
}

impl<T> From<TypedVar<T>> for ColVal
where
    ColType: FromType<T>,
{
    fn from(value: TypedVar<T>) -> Self {
        Self::Binding(value.into())
    }
}

impl<T> From<&TypedVar<T>> for ColVal
where
    ColType: FromType<T>,
    T: Copy,
{
    fn from(value: &TypedVar<T>) -> Self {
        Self::Binding((*value).into())
    }
}

impl Display for ColVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColVal::Lit(inner) => Display::fmt(&inner, f),
            ColVal::Binding(inner) => Display::fmt(&inner, f),
        }
    }
}

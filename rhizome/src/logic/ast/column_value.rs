use std::fmt::{self, Display};

use crate::value::Value;

use super::Var;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ColumnValue {
    Literal(Value),
    Binding(Var),
}

impl From<Value> for ColumnValue {
    fn from(value: Value) -> Self {
        Self::Literal(value)
    }
}

impl From<Var> for ColumnValue {
    fn from(value: Var) -> Self {
        Self::Binding(value)
    }
}

impl From<&Var> for ColumnValue {
    fn from(value: &Var) -> Self {
        Self::Binding(*value)
    }
}

impl Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnValue::Literal(inner) => Display::fmt(&inner, f),
            ColumnValue::Binding(inner) => Display::fmt(&inner, f),
        }
    }
}

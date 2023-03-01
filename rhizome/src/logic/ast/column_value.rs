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

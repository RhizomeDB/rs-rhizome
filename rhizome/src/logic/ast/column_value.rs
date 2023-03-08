use derive_more::From;

use crate::value::Value;

use super::Var;

#[derive(Debug, Clone, Eq, From, PartialEq)]
pub enum ColumnValue {
    Literal(Value),
    Binding(Var),
}

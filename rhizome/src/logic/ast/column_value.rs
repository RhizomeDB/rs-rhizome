use derive_more::{From, IsVariant, TryInto};

use crate::{id::VarId, value::Value};

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum ColumnValue {
    Literal(Value),
    Binding(VarId),
}

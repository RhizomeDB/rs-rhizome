use cid::Cid;
use derive_more::{IsVariant, TryInto};

use crate::{
    types::{FromType, Type},
    var::{TypedVar, Var},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, IsVariant, TryInto)]
pub enum CidValue {
    Cid(Cid),
    Var(Var),
}

impl From<Cid> for CidValue {
    fn from(value: Cid) -> Self {
        Self::Cid(value)
    }
}

impl From<&Var> for CidValue {
    fn from(value: &Var) -> Self {
        Self::Var(*value)
    }
}

impl<T> From<&TypedVar<T>> for CidValue
where
    Type: FromType<T>,
    T: Copy,
{
    fn from(value: &TypedVar<T>) -> Self {
        Self::Var((*value).into())
    }
}

use cid::Cid;
use derive_more::{IsVariant, TryInto};

use crate::id::VarId;

use super::Var;

#[derive(Debug, Clone, Copy, Eq, PartialEq, IsVariant, TryInto)]
pub enum CidValue {
    Cid(Cid),
    Var(VarId),
}

impl From<Cid> for CidValue {
    fn from(value: Cid) -> Self {
        Self::Cid(value)
    }
}

impl From<&Var> for CidValue {
    fn from(value: &Var) -> Self {
        Self::Var(value.id())
    }
}

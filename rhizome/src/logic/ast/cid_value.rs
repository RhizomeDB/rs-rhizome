use cid::Cid;
use derive_more::{IsVariant, TryInto};

use crate::id::VarId;

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

impl From<VarId> for CidValue {
    fn from(value: VarId) -> Self {
        Self::Var(value)
    }
}

impl From<&str> for CidValue {
    fn from(value: &str) -> Self {
        let value = VarId::new(value);

        Self::Var(value)
    }
}

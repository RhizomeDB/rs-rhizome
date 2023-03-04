use cid::Cid;
use derive_more::{From, IsVariant, TryInto};

use crate::id::VarId;

#[derive(Debug, Clone, Copy, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum CidValue {
    Cid(Cid),
    Variable(VarId),
}

use cid::Cid;
use derive_more::{IsVariant, TryInto};

use crate::{
    types::{ColType, FromType},
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

impl<T> From<TypedVar<T>> for CidValue
where
    ColType: FromType<T>,
{
    fn from(value: TypedVar<T>) -> Self {
        let var = if let ColType::Any = value.typ() {
            Var::new::<Cid>(value.id().to_string().as_ref())
        } else {
            Var::new::<T>(value.id().to_string().as_ref())
        };

        Self::Var(var)
    }
}

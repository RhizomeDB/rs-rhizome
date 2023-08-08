use crate::{col_val::ColVal, id::ColId, types::IntoColType, value::Val, var::TypedVar};

pub trait AtomBinding {
    fn into_pair(self) -> (ColId, ColVal);
}

impl<C, T> AtomBinding for (C, TypedVar<T>)
where
    C: Into<ColId>,
    T: IntoColType,
{
    fn into_pair(self) -> (ColId, ColVal) {
        (self.0.into(), ColVal::Binding(self.1.into()))
    }
}

impl<C, T> AtomBinding for (C, T)
where
    C: Into<ColId>,
    T: Into<Val>,
{
    fn into_pair(self) -> (ColId, ColVal) {
        (self.0.into(), ColVal::Lit(self.1.into()))
    }
}

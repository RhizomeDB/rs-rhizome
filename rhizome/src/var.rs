use std::{
    fmt::{self, Display},
    marker::PhantomData,
};

use crate::{
    id::VarId,
    types::{ColType, IntoColType},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Var {
    id: VarId,
    typ: ColType,
}

impl Var {
    pub fn new<T>(id: &str) -> Self
    where
        T: IntoColType,
    {
        let id = VarId::new(id);
        let typ = T::into_col_type();

        Self { id, typ }
    }

    pub fn id(&self) -> VarId {
        self.id
    }

    pub fn typ(&self) -> ColType {
        self.typ
    }
}

impl<T> From<TypedVar<T>> for Var
where
    T: IntoColType,
{
    fn from(value: TypedVar<T>) -> Self {
        Self::new::<T>(value.id().to_string().as_ref())
    }
}

impl Display for Var {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("({} : {})", self.id, self.typ))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TypedVar<T> {
    id: VarId,
    typ: ColType,
    _marker: PhantomData<T>,
}

impl<T> TypedVar<T>
where
    T: IntoColType,
{
    pub fn new(id: &str) -> Self {
        let id = VarId::new(id);
        let typ = T::into_col_type();

        Self {
            id,
            typ,
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> VarId {
        self.id
    }

    pub fn typ(&self) -> ColType {
        self.typ
    }

    pub fn as_var(&self) -> Var {
        Var::new::<T>(self.id().to_string().as_ref())
    }
}

impl<T> Display for TypedVar<T>
where
    T: IntoColType,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("({} : {})", self.id, self.typ))
    }
}

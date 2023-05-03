use std::{
    fmt::{self, Display},
    marker::PhantomData,
};

use crate::{
    id::VarId,
    types::{ColType, FromType},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Var {
    id: VarId,
    typ: ColType,
}

impl Var {
    pub fn new<T>(id: &str) -> Self
    where
        ColType: FromType<T>,
    {
        let id = VarId::new(id);
        let typ = FromType::<T>::from_type();

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
    ColType: FromType<T>,
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
    ColType: FromType<T>,
{
    pub fn new(id: &str) -> Self {
        let id = VarId::new(id);
        let typ = FromType::<T>::from_type();

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
}

impl<T> Display for TypedVar<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("({} : {})", self.id, self.typ))
    }
}

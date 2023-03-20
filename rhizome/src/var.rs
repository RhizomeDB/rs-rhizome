use std::fmt::{self, Display};

use crate::{
    id::VarId,
    types::{FromType, Type},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Var {
    id: VarId,
    typ: Type,
}

impl Var {
    pub fn new<T>(id: &str) -> Self
    where
        Type: FromType<T>,
    {
        let id = VarId::new(id);
        let typ = FromType::<T>::from_type();

        Self { id, typ }
    }

    pub fn id(&self) -> VarId {
        self.id
    }

    pub fn typ(&self) -> Type {
        self.typ
    }
}

impl Display for Var {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("({} : {})", self.id, self.typ))
    }
}

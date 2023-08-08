use core::fmt::{self, Debug};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt::Display, marker::PhantomData};

use crate::interner::Symbol;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T, U>(Symbol, PhantomData<(T, U)>);

impl<T, U> Debug for Id<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl<T, U> Id<T, U> {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        let symbol = Symbol::get_or_intern(id.as_ref());

        Self(symbol, PhantomData)
    }

    pub fn resolve(&self) -> String {
        self.0.resolve()
    }
}

impl<T, U> Display for Id<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resolve())
    }
}

impl<T, U, S> From<S> for Id<T, U>
where
    S: Borrow<str>,
{
    fn from(id: S) -> Self {
        Self::new(id.borrow())
    }
}

#[macro_export]
macro_rules! new_id {
    ($name:ident) => {
        paste::item! {
            #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
            pub enum [< $name Marker >] {}

            pub(crate) type $name = Id<[< $name Marker >], ()>;
        }
    };

    ($name:ident<$t:ident>) => {
        paste::item! {
            #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
            pub enum [< $name Marker >] {}

            pub type $name<$t> = Id<[< $name Marker >], $t>;
        }
    };
}

new_id!(ColId);
new_id!(RelationId);
new_id!(VarId);

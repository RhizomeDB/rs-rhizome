use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt::Display, marker::PhantomData};

use crate::interner::Symbol;

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Id<T, U>(Symbol, PhantomData<(T, U)>);

impl<T, U> Id<T, U> {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        let symbol = Symbol::get_or_intern(id.as_ref());

        Self(symbol, PhantomData::default())
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

            pub type $name = Id<[< $name Marker >], ()>;
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
new_id!(LinkId);
new_id!(RelationId);
new_id!(VarId);

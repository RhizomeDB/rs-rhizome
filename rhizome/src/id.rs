use serde::Deserialize;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::interner::Symbol;

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Id<T>(Symbol, PhantomData<T>);

impl<T> Id<T> {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        let symbol = Symbol::get_or_intern(id.as_ref());

        Self(symbol, PhantomData::default())
    }

    pub fn resolve(&self) -> String {
        self.0.resolve()
    }
}

impl<T> Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resolve())
    }
}

impl<T, S: Borrow<str>> From<S> for Id<T> {
    fn from(id: S) -> Self {
        Self::new(id.borrow())
    }
}

macro_rules! new_id {
    ($name:ident) => {
        paste::item! {
            #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
            pub enum [< $name Marker >] {}

            pub type $name = Id<[< $name Marker >]>;
        }
    };
}

new_id!(AttributeId);
new_id!(LinkId);
new_id!(RelationId);
new_id!(VariableId);

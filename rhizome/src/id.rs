use std::fmt::Display;

use crate::interner::{self, Symbol};

macro_rules! new_id {
    ($name:ident) => {
        #[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Symbol);

        impl $name {
            pub fn new<T: AsRef<str>>(id: T) -> Self {
                let symbol = interner::get_or_intern(id.as_ref());

                Self(symbol)
            }

            pub fn resolve(&self) -> String {
                interner::resolve(self.0)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.resolve())
            }
        }

        impl From<&str> for $name {
            fn from(id: &str) -> Self {
                Self::new(id)
            }
        }
    };
}

new_id!(AttributeId);
new_id!(RelationId);
new_id!(VariableId);

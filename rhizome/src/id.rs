use std::fmt::Display;

use crate::interner::{self, Symbol};

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttributeId(Symbol);

impl AttributeId {
    pub fn new(id: &str) -> Self {
        let symbol = interner::get_or_intern(id);

        Self(symbol)
    }

    pub fn resolve(&self) -> String {
        interner::resolve(self.0)
    }
}

impl Display for AttributeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resolve())
    }
}

impl From<&str> for AttributeId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct RelationId(Symbol);

impl RelationId {
    pub fn new(id: &str) -> Self {
        let symbol = interner::get_or_intern(id);

        Self(symbol)
    }

    pub fn resolve(&self) -> String {
        interner::resolve(self.0)
    }
}

impl Display for RelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resolve())
    }
}

impl From<&str> for RelationId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct VariableId(Symbol);

impl VariableId {
    pub fn new(id: &str) -> Self {
        let symbol = interner::get_or_intern(id);

        Self(symbol)
    }

    pub fn resolve(&self) -> String {
        interner::resolve(self.0)
    }
}

impl Display for VariableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resolve())
    }
}

impl From<&str> for VariableId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

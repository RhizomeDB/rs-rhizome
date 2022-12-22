use std::fmt::Display;
use ustr::Ustr;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttributeId(Ustr);

impl AttributeId {
    pub fn new(id: &str) -> Self {
        let symbol = Ustr::from(id);

        Self(symbol)
    }
}

impl Display for AttributeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl From<&str> for AttributeId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelationId(Ustr);

impl RelationId {
    pub fn new(id: &str) -> Self {
        let symbol = Ustr::from(id);

        Self(symbol)
    }
}

impl Display for RelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl From<&str> for RelationId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VariableId(Ustr);

impl VariableId {
    pub fn new(id: &str) -> Self {
        let symbol = Ustr::from(id);

        Self(symbol)
    }
}

impl Display for VariableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl From<&str> for VariableId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

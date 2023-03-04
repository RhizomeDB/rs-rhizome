use derive_more::IsVariant;

use crate::id::RelationId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Node {
    EDB(RelationId),
    IDB(RelationId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Edge {
    FromEDB(RelationId, RelationId, Polarity),
    FromIDB(RelationId, RelationId, Polarity),
}

impl Edge {
    pub fn from(&self) -> Node {
        match *self {
            Edge::FromEDB(from, _, _) => Node::EDB(from),
            Edge::FromIDB(from, _, _) => Node::IDB(from),
        }
    }

    pub fn to(&self) -> Node {
        match *self {
            Edge::FromEDB(_, to, _) => Node::IDB(to),
            Edge::FromIDB(_, to, _) => Node::IDB(to),
        }
    }

    pub fn polarity(&self) -> Polarity {
        match *self {
            Edge::FromEDB(_, _, polarity) => polarity,
            Edge::FromIDB(_, _, polarity) => polarity,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, IsVariant)]
pub enum Polarity {
    Positive,
    Negative,
}

use std::collections::HashMap;

use crate::{
    id::{ColumnId, RelationId},
    value::Value,
};

use super::Edge;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Fact {
    head: RelationId,
    args: HashMap<ColumnId, Value>,
}

impl Fact {
    pub fn new(head: RelationId, args: HashMap<ColumnId, Value>) -> Self {
        Self { head, args }
    }

    pub fn head(&self) -> RelationId {
        self.head
    }

    pub fn args(&self) -> &HashMap<ColumnId, Value> {
        &self.args
    }

    pub fn depends_on(&self) -> Vec<Edge> {
        Vec::default()
    }
}

use std::collections::HashMap;

use crate::{
    id::{ColId, RelationId},
    value::Val,
};

use super::Edge;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Fact {
    head: RelationId,
    args: HashMap<ColId, Val>,
}

impl Fact {
    pub fn new(head: RelationId, args: HashMap<ColId, Val>) -> Self {
        Self { head, args }
    }

    pub fn head(&self) -> RelationId {
        self.head
    }

    pub fn args(&self) -> &HashMap<ColId, Val> {
        &self.args
    }

    pub fn depends_on(&self) -> Vec<Edge> {
        Vec::default()
    }
}

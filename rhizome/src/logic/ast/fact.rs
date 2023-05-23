use std::collections::HashMap;

use crate::{
    id::{ColId, RelationId},
    value::Val,
};

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
}

use std::collections::HashMap;

use crate::{
    col::Col,
    id::{ColId, RelationId},
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    id: RelationId,
    cols: HashMap<ColId, Col>,
}

impl Schema {
    pub fn new(id: RelationId, cols: HashMap<ColId, Col>) -> Self {
        Self { id, cols }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn has_col(&self, k: &ColId) -> bool {
        self.cols().contains_key(k)
    }

    pub fn get_col(&self, k: &ColId) -> Option<&Col> {
        self.cols.get(k)
    }

    pub fn cols(&self) -> &HashMap<ColId, Col> {
        &self.cols
    }
}

use crate::{id::ColId, types::ColType};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Col {
    id: ColId,
    col_type: ColType,
}

impl Col {
    pub fn new(id: ColId, col_type: ColType) -> Self {
        Self { id, col_type }
    }

    pub fn id(&self) -> ColId {
        self.id
    }

    pub fn col_type(&self) -> &ColType {
        &self.col_type
    }
}

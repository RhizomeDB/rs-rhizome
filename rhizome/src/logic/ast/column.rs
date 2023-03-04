use crate::{id::ColumnId, types::ColumnType};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Column {
    id: ColumnId,
    column_type: ColumnType,
}

impl Column {
    pub fn new(id: ColumnId, column_type: ColumnType) -> Self {
        Self { id, column_type }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }
}

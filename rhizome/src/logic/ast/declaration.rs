use crate::{id::RelationId, relation::Source};
use std::sync::Arc;

use super::Schema;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Declaration {
    id: RelationId,
    schema: Arc<Schema>,
    source: Source,
}

impl Declaration {
    pub fn new(id: RelationId, schema: Arc<Schema>, source: Source) -> Self {
        Self { id, schema, source }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn source(&self) -> Source {
        self.source
    }
}

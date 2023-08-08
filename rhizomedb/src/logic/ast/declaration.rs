use crate::{
    id::RelationId,
    relation::{Relation, Source},
};
use std::sync::Arc;

use super::Schema;

#[derive(Debug, Clone)]
pub struct Declaration {
    id: RelationId,
    schema: Arc<Schema>,
    source: Source,
    relation: Box<dyn Relation>,
}

impl Declaration {
    pub fn new(
        id: RelationId,
        schema: Arc<Schema>,
        source: Source,
        relation: Box<dyn Relation>,
    ) -> Self {
        Self {
            id,
            schema,
            source,
            relation,
        }
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

    pub fn relation(&self) -> Box<dyn Relation> {
        dyn_clone::clone_box(&*self.relation)
    }
}

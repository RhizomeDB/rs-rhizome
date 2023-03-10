use pretty::RcDoc;

use crate::{pretty::Pretty, ram::operation::Operation};

#[derive(Clone, Debug)]
pub struct Insert {
    operation: Operation,
    // Whether the insertion is for a ground atom with all constant columns.
    // I don't love this, but it enables us to ensure ground facts are only inserted
    // into the delta relation once.
    is_ground: bool,
}

impl Insert {
    pub fn new(operation: Operation, is_ground: bool) -> Self {
        Self {
            operation,
            is_ground,
        }
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    pub fn is_ground(&self) -> bool {
        self.is_ground
    }
}

impl Pretty for Insert {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::text("insert").append(
            RcDoc::hardline()
                .append(self.operation().to_doc())
                .nest(2)
                .group(),
        )
    }
}

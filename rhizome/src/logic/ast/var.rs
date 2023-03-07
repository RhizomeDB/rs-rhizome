use crate::id::VarId;

#[derive(Debug)]
pub struct Var {
    // TODO: I'd love for this to be VarId, but we need to create these statically,
    // and VarId requires the intern table
    id: usize,
}

impl Var {
    pub const fn new(id: usize) -> Self {
        Self { id }
    }

    pub fn id(&self) -> VarId {
        VarId::new(format!("X{}", self.id))
    }
}

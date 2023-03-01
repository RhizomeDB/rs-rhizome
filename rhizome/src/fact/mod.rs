use self::{btree_fact::BTreeFact, evac_fact::EVACFact};

pub mod btree_fact;
pub mod evac_fact;
pub mod traits;

pub type DefaultEDBFact = EVACFact;
pub type DefaultIDBFact = BTreeFact;

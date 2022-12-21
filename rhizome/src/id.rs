use derive_more::{AsRef, Constructor, Display, From};
use serde::{Deserialize, Serialize};

#[derive(Constructor, Display, Clone, Debug, From, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AliasId(usize);

impl AliasId {
    pub fn next(&self) -> Self {
        AliasId::new(self.0 + 1)
    }
}

#[derive(
    Constructor,
    Display,
    From,
    Serialize,
    Deserialize,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[from(forward)]
#[serde(transparent)]
pub struct AttributeId(String);

#[derive(
    Constructor,
    Display,
    From,
    Serialize,
    Deserialize,
    AsRef,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[from(forward)]
#[serde(transparent)]
pub struct RelationId(String);

#[derive(
    Constructor,
    Display,
    From,
    Serialize,
    Deserialize,
    AsRef,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[from(forward)]
#[serde(transparent)]
pub struct VariableId(String);

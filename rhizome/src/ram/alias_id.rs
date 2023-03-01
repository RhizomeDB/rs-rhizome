use derive_more::Display;

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AliasId(usize);

impl AliasId {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for AliasId {
    fn default() -> Self {
        Self::new()
    }
}

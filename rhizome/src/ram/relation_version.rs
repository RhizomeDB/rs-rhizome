use std::fmt::{self, Display};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationVersion {
    Total,
    Delta,
    New,
}

impl Display for RelationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelationVersion::Total => f.write_str("total"),
            RelationVersion::Delta => f.write_str("delta"),
            RelationVersion::New => f.write_str("new"),
        }
    }
}

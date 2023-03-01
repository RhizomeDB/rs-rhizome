//! Rhizome errors

use anyhow::Result;
use thiserror::Error;

use crate::id::{AttributeId, RelationId, VariableId};

/// Rhizome errors.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum Error {
    #[error("Unknown Error")]
    Unknown,
    #[error("Cannot find a node with the specified CID in block store")]
    CIDNotFoundInBlockstore,
    #[error("Program could not be parsed")]
    // TODO: Include diagnostics on this error
    ProgramParseError,
    #[error("Program could not be stratified")]
    ProgramUnstratifiable,
    #[error("Rule not range restricted: variable {1}, in attribute {0} of rule head must be bound by a positive body term")]
    RuleNotRangeRestricted(AttributeId, VariableId),
    #[error("Rule not domain independent: variable {2}, in attribute {1} of negated atom {0} must be bound by a positive body term")]
    RuleNotDomainIndependent(RelationId, AttributeId, VariableId),
    #[error("Error while pulling from source")]
    SourcePullError,
    #[error("Error while pushing to sink")]
    SinkPushError,
    #[error("Duplicate attribute ID specified in relation schema: {0}")]
    DuplicateSchemaAttributeId(AttributeId),
}

pub fn error<T>(err: impl std::error::Error + Send + Sync + 'static) -> Result<T> {
    Err(err.into())
}

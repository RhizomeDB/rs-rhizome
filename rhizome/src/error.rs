//! Rhizome errors

use anyhow::Result;
use thiserror::Error;

use crate::{
    id::{ColumnId, RelationId, VarId},
    types::{ColumnType, Type},
};

/// Rhizome errors.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum Error {
    // #[error("Program could not be parsed")]
    // TODO: Include diagnostics on this error
    // ProgramParseError,
    #[error("Program could not be stratified")]
    ProgramUnstratifiable,
    #[error("Clause not range restricted: variable {1}, in attribute {0} of head must be bound")]
    ClauseNotRangeRestricted(ColumnId, VarId),
    #[error("Clause not domain independent: variable {0} must be bound")]
    ClauseNotDomainIndependent(VarId),
    #[error("Error while pulling from source")]
    SourcePullError,
    #[error("Error while pushing to sink")]
    SinkPushError,
    #[error("Duplicate attribute ID specified in relation schema: {0}")]
    DuplicateSchemaAttributeId(ColumnId),
    #[error("Relation already declared: {0}")]
    ConflictingRelationDeclaration(RelationId),
    #[error("Column already bound: {0}")]
    ConflictingColumnBinding(ColumnId),
    #[error("Unrecognized column: {0}, for relation {1}")]
    UnrecognizedColumnBinding(ColumnId, RelationId),
    #[error("Unexpected type for column {0}, of relation {1}")]
    UnexpectedColumnBindingType(ColumnId, RelationId),
    #[error("Column missing: {0}, for relation {1}")]
    ColumnMissing(ColumnId, RelationId),
    #[error("Unrecognized relation: {0}")]
    UnrecognizedRelation(String),
    #[error("Clause head must be an output relation: {0}")]
    ClauseHeadEDB(RelationId),
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(Type, Type),
    #[error("Attempted to bind {0}, of type {2}, to column of type {1}")]
    VariableTypeConflict(VarId, ColumnType, Type),
}

pub fn error<T>(err: impl std::error::Error + Send + Sync + 'static) -> Result<T> {
    Err(err.into())
}

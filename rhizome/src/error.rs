//! Rhizome errors

use anyhow::Result;
use thiserror::Error;

use crate::{
    id::{ColumnId, RelationId, VarId},
    logic::ast::{ColumnValue, Var},
    types::{ColumnType, Type},
};

/// Rhizome errors.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum Error {
    #[error("Program could not be stratified")]
    ProgramUnstratifiable,
    #[error("Clause not range restricted: variable {1}, in attribute {0} of head must be bound")]
    ClauseNotRangeRestricted(ColumnId, VarId),
    #[error("Clause not domain independent: variable {0} must be bound")]
    ClauseNotDomainIndependent(VarId),
    #[error("Duplicate column: {1} in schema for relation {0}")]
    DuplicateDeclarationColumn(RelationId, ColumnId),
    #[error("Relation already declared: {0}")]
    ConflictingRelationDeclaration(RelationId),
    #[error("Column {1} already bound in clause head, for relation {1}")]
    ConflictingColumnBinding(RelationId, ColumnId),
    #[error("Unrecognized column: {1}, for relation {0}")]
    UnrecognizedColumnBinding(RelationId, ColumnId),
    #[error("Column missing: {1}, for relation {0}")]
    ColumnMissing(RelationId, ColumnId),
    #[error("Unrecognized relation: {0}")]
    UnrecognizedRelation(String),
    #[error("Clause head must be an output relation: {0}")]
    ClauseHeadEDB(RelationId),
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(Type, Type),
    #[error("Type mismatch: expected {0}, got {1}")]
    VarTypeConflict(Var, Type),
    #[error("Attempted to bind {2} to {1} of type {3} in {0}")]
    ColumnValueTypeConflict(RelationId, ColumnId, ColumnValue, ColumnType),
    #[error("Facts must be ground: attempted to bind {1} to variable {2} of relation {0}")]
    NonGroundFact(RelationId, ColumnId, VarId),
}

pub fn error<T>(err: impl std::error::Error + Send + Sync + 'static) -> Result<T> {
    Err(err.into())
}

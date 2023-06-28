//! Rhizome errors

use anyhow::Result;
use thiserror::Error;

use crate::{
    col_val::ColVal,
    id::{ColId, RelationId, VarId},
    types::{ColType, Type},
};

/// Rhizome errors.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum Error {
    #[error(
        "An unexpected error occurred in Rhizome: {0}. This is a bug: please consider filing an issue"
    )]
    InternalRhizomeError(String),
    #[error("Program could not be stratified")]
    ProgramUnstratifiable,
    #[error("Clause not range restricted: variable {1}, in attribute {0} of head must be bound")]
    ClauseNotRangeRestricted(ColId, VarId),
    #[error("Clause not domain independent: variable {0} must be bound")]
    ClauseNotDomainIndependent(VarId),
    #[error("Duplicate column: {1} in schema for relation {0}")]
    DuplicateDeclarationCol(RelationId, ColId),
    #[error("Relation already declared: {0}")]
    ConflictingRelationDeclaration(RelationId),
    #[error("Column {1} already bound in clause head, for relation {1}")]
    ConflictingColumnBinding(RelationId, ColId),
    #[error("Unrecognized column: {1}, for relation {0}")]
    UnrecognizedColumnBinding(RelationId, ColId),
    #[error("Column missing: {1}, for relation {0}")]
    ColumnMissing(RelationId, ColId),
    #[error("Unrecognized relation: {0}")]
    UnrecognizedRelation(String),
    #[error("Clause head must be an output relation: {0}")]
    ClauseHeadEDB(RelationId),
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(Type, Type),
    #[error("Attempted to bind {2} to {1} of type {3} in {0}")]
    ColumnValueTypeConflict(RelationId, ColId, ColVal, ColType),
    #[error("Facts must be ground: attempted to bind {1} to variable {2} of relation {0}")]
    NonGroundFact(RelationId, ColId, VarId),
    #[error("Attempted to group by unbound variable {0} for column {1} of relation {2}")]
    AggregationUnboundGroupBy(VarId, ColId, RelationId),
    #[error("Attempted to aggregate into a bound variable {0}")]
    AggregationBoundTarget(VarId),
    #[error("Attempted to bind to CID of IDB relation {0}")]
    ContentAddressedIDB(RelationId),
}

pub fn error<T>(err: impl std::error::Error + Send + Sync + 'static) -> Result<T> {
    Err(err.into())
}

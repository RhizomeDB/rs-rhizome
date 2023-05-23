mod ast;
mod builder;

pub(crate) mod lower_to_ram;
pub(crate) mod stratify;

pub use builder::{build, ProgramBuilder, RuleVars};

use crate::value::Val;

pub trait VarClosure: Fn(Vec<Val>) -> Result<bool, ()> + Send + Sync {}
impl<T> VarClosure for T where T: Fn(Vec<Val>) -> Result<bool, ()> + Send + Sync {}

pub trait AggregationClosure: Fn(Val, Vec<Val>) -> Result<Val, ()> + Send + Sync {}
impl<T> AggregationClosure for T where T: Fn(Val, Vec<Val>) -> Result<Val, ()> + Send + Sync {}

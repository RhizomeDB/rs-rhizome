mod ast;
mod builder;

pub(crate) mod lower_to_ram;

use anyhow::Result;
pub(crate) use builder::build;

pub use builder::{ProgramBuilder, RuleVars};

use crate::value::Val;

pub trait VarClosure: Fn(Vec<Val>) -> Result<bool> + Send + Sync {}
impl<T> VarClosure for T where T: Fn(Vec<Val>) -> Result<bool> + Send + Sync {}

pub trait ReduceClosure: Fn(Val, Vec<Val>) -> Result<Val> + Send + Sync {}
impl<T> ReduceClosure for T where T: Fn(Val, Vec<Val>) -> Result<Val> + Send + Sync {}

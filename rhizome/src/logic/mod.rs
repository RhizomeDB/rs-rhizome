mod ast;
mod builder;

pub(crate) mod lower_to_ram;

pub(crate) use builder::build;
pub use builder::ProgramBuilder;

use crate::value::Val;

pub trait VarClosure: Fn(Vec<Val>) -> bool + Send + Sync {}
impl<T> VarClosure for T where T: Fn(Vec<Val>) -> bool + Send + Sync {}

pub trait ReduceClosure: Fn(Val, Vec<Val>) -> Val + Send + Sync {}
impl<T> ReduceClosure for T where T: Fn(Val, Vec<Val>) -> Val + Send + Sync {}

mod ast;
mod builder;

pub(crate) mod lower_to_ram;
pub(crate) mod stratify;

pub use builder::{
    build, AtomBinding, AtomBindings, ProgramBuilder, RuleBodyBuilder, RuleVars, TypedVars,
};

use crate::value::Val;

pub trait VarClosure: Fn(Vec<Val>) -> Result<bool, ()> + Send + Sync {}
impl<T> VarClosure for T where T: Fn(Vec<Val>) -> Result<bool, ()> + Send + Sync {}

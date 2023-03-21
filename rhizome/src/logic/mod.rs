mod ast;
mod builder;

pub(crate) mod lower_to_ram;

pub use builder::build;

use crate::value::Val;

pub trait VarClosure: Fn(Vec<Val>) -> bool + Send + Sync {}

impl<T> VarClosure for T where T: Fn(Vec<Val>) -> bool + Send + Sync {}

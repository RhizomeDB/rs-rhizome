mod ast;
mod builder;

pub(crate) mod lower_to_ram;

use std::sync::Arc;

pub use builder::build;

use crate::value::Val;

pub trait VarClosure: Fn(&[Arc<Val>]) -> bool + Send + Sync {}

impl<T> VarClosure for T where T: Fn(&[Arc<Val>]) -> bool + Send + Sync {}

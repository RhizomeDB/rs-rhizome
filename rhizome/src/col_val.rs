use std::{
    fmt::{self, Display},
    sync::Arc,
};

use crate::{value::Val, var::Var};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ColVal {
    Lit(Arc<Val>),
    Binding(Var),
}

impl Display for ColVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColVal::Lit(inner) => Display::fmt(&inner, f),
            ColVal::Binding(inner) => Display::fmt(&inner, f),
        }
    }
}

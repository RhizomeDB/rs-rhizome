use pretty::RcDoc;
use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use crate::{logic::VarClosure, pretty::Pretty, value::Val};

use super::Term;

pub struct Predicate {
    args: Vec<Term>,
    f: Arc<dyn VarClosure>,
}

impl Predicate {
    pub fn new(args: Vec<Term>, f: Arc<dyn VarClosure>) -> Self {
        Self { args, f }
    }

    pub fn args(&self) -> &Vec<Term> {
        &self.args
    }

    pub fn is_satisfied(&self, args: Vec<Val>) -> bool {
        (self.f)(args)
    }
}

impl Pretty for Predicate {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let args_doc = RcDoc::intersperse(
            self.args().iter().map(|arg| arg.to_doc()),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(1)
        .group();

        RcDoc::concat([RcDoc::text("UDF("), args_doc, RcDoc::text(")")])
    }
}

impl Debug for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Predicate")
            .field("args", &self.args)
            .finish()
    }
}

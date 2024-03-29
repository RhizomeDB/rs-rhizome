use anyhow::Result;
use pretty::RcDoc;
use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use crate::{error::Error, predicate::PredicateWrapper, pretty::Pretty, value::Val};

use super::Term;

#[derive(Clone)]
pub(crate) struct Predicate {
    args: Vec<Term>,
    f: Arc<dyn PredicateWrapper>,
}

impl Predicate {
    pub(crate) fn new(args: Vec<Term>, f: Arc<dyn PredicateWrapper>) -> Self {
        Self { args, f }
    }

    pub(crate) fn args(&self) -> &Vec<Term> {
        &self.args
    }

    pub(crate) fn is_satisfied(&self, args: Vec<Val>) -> Result<bool> {
        Ok(self
            .f
            .apply(args)
            .ok_or_else(|| Error::InternalRhizomeError("failed to apply predicate".to_owned()))?)
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

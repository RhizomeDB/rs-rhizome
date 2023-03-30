use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use pretty::RcDoc;

use crate::{
    id::ColId,
    logic::ReduceClosure,
    pretty::Pretty,
    ram::{AliasId, Formula, RelationRef, Term},
    value::Val,
    var::Var,
};

use super::Operation;

pub struct Reduce {
    args: Vec<Term>,
    init: Val,
    f: Arc<dyn ReduceClosure>,
    group_by_cols: HashMap<ColId, Term>,
    target: Var,
    relation: RelationRef,
    alias: Option<AliasId>,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl Reduce {
    pub fn new(
        args: Vec<Term>,
        init: Val,
        f: Arc<dyn ReduceClosure>,
        target: Var,
        group_by_cols: HashMap<ColId, Term>,
        relation: RelationRef,
        alias: Option<AliasId>,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            args,
            init,
            f,
            target,
            group_by_cols,
            relation,
            alias,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn args(&self) -> &Vec<Term> {
        &self.args
    }

    pub fn init(&self) -> &Val {
        &self.init
    }

    pub fn apply(&self, acc: Val, args: Vec<Val>) -> Val {
        (self.f)(acc, args)
    }

    pub fn group_by_cols(&self) -> &HashMap<ColId, Term> {
        &self.group_by_cols
    }

    pub fn target(&self) -> Var {
        self.target
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }

    pub fn alias(&self) -> &Option<AliasId> {
        &self.alias
    }

    pub fn when(&self) -> &[Formula] {
        &self.when
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

impl Debug for Reduce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reduce")
            .field("args", &self.args)
            .field("group_by_cols", &self.group_by_cols)
            .field("target", &self.target)
            .field("relation", &self.relation)
            .field("alias", &self.alias)
            .field("when", &self.when)
            .field("operation", &self.operation)
            .finish()
    }
}

impl Pretty for Reduce {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        // TODO
        RcDoc::concat([RcDoc::text("TODO REDUCE "), self.operation().to_doc()])
    }
}

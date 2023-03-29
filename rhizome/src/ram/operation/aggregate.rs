use std::collections::HashMap;

use pretty::RcDoc;

use crate::{
    aggregation_function::AggregationFunction,
    id::ColId,
    pretty::Pretty,
    ram::{AliasId, Formula, RelationRef, Term},
};

use super::Operation;

#[derive(Debug)]
pub struct Aggregate {
    function: AggregationFunction,
    group_by_cols: HashMap<ColId, Term>,
    target_col: ColId,
    relation: RelationRef,
    alias: Option<AliasId>,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl Aggregate {
    pub fn new(
        function: AggregationFunction,
        target_col: ColId,
        group_by_cols: HashMap<ColId, Term>,
        relation: RelationRef,
        alias: Option<AliasId>,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            function,
            target_col,
            group_by_cols,
            relation,
            alias,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn function(&self) -> AggregationFunction {
        self.function
    }

    pub fn group_by_cols(&self) -> &HashMap<ColId, Term> {
        &self.group_by_cols
    }

    pub fn target_col(&self) -> ColId {
        self.target_col
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

impl Pretty for Aggregate {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        // TODO
        RcDoc::concat([RcDoc::text("TODO AGGREGATION "), self.operation().to_doc()])
    }
}

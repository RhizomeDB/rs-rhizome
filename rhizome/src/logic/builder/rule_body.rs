use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, fmt::Debug, rc::Rc, sync::Arc};

use crate::{
    aggregation::{AggAcc, AggregateGroupBy, AggregateWrapper},
    args::Args,
    error::{error, Error},
    id::VarId,
    logic::ast::{BodyTerm, CidValue, Declaration, VarPredicate},
    predicate::{PredicateWhere, PredicateWrapper},
    types::ColType,
    var::{TypedVar, Var},
};

use super::{
    aggregation::AggregationBuilder, atom_bindings::AtomBindings, negation::NegationBuilder,
    rel_predicate::RelPredicateBuilder,
};

type RelPredicates = Vec<(String, RelPredicateBuilder)>;
type Negations = Vec<(String, NegationBuilder)>;
type VarPredicates = Vec<(Vec<Var>, Arc<dyn PredicateWrapper>)>;
type Aggregations = Vec<(String, AggregationBuilder)>;
type Relations = HashMap<String, Arc<Declaration>>;

pub struct RuleBodyBuilder {
    rel_predicates: RefCell<RelPredicates>,
    negations: RefCell<Negations>,
    var_predicates: RefCell<VarPredicates>,
    aggregations: RefCell<Aggregations>,
    relations: Rc<RefCell<Relations>>,
}

impl Debug for RuleBodyBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleBodyBuilder").finish()
    }
}

impl RuleBodyBuilder {
    pub fn new(relations: Rc<RefCell<HashMap<String, Arc<Declaration>>>>) -> Self {
        Self {
            rel_predicates: RefCell::default(),
            negations: RefCell::default(),
            var_predicates: RefCell::default(),
            aggregations: RefCell::default(),
            relations,
        }
    }

    pub fn finalize(self, bound_vars: &mut HashMap<VarId, ColType>) -> Result<Vec<BodyTerm>> {
        let mut body_terms = Vec::default();

        for (id, builder) in self.rel_predicates.into_inner() {
            let Some(declaration) = self.relations.borrow().get(&id).cloned() else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let predicate = builder.finalize(declaration, bound_vars)?;
            let term = BodyTerm::RelPredicate(predicate);

            body_terms.push(term);
        }

        for (vars, f) in self.var_predicates.into_inner() {
            for var in &vars {
                if !bound_vars.contains_key(&var.id()) {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            let term = BodyTerm::VarPredicate(VarPredicate::new(vars, f));

            body_terms.push(term);
        }

        for (id, builder) in self.negations.into_inner() {
            let Some(declaration) = self.relations.borrow().get(&id).cloned() else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let negation = builder.finalize(declaration)?;

            for var in negation.vars() {
                if !bound_vars.contains_key(&var.id()) {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            let term = BodyTerm::Negation(negation);

            body_terms.push(term);
        }

        for (id, builder) in self.aggregations.into_inner() {
            let Some(declaration) = self.relations.borrow().get(&id).cloned() else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let aggregation = builder.finalize(declaration, bound_vars)?;
            let term = BodyTerm::Aggregation(aggregation);

            body_terms.push(term);
        }

        Ok(body_terms)
    }

    pub fn search<T>(&self, id: &str, bindings: T) -> Result<()>
    where
        T: AtomBindings,
    {
        let builder = RelPredicateBuilder::new(None);

        bindings.bind(&mut builder.bindings.borrow_mut());

        self.rel_predicates
            .borrow_mut()
            .push((id.to_string(), builder));

        Ok(())
    }

    pub fn search_cid<C, T>(&self, id: &str, cid: C, bindings: T) -> Result<()>
    where
        C: Into<CidValue>,
        T: AtomBindings,
    {
        let builder = RelPredicateBuilder::new(Some(cid.into()));

        bindings.bind(&mut builder.bindings.borrow_mut());

        self.rel_predicates
            .borrow_mut()
            .push((id.to_string(), builder));

        Ok(())
    }

    pub fn build_search<F>(&self, id: &str, cid: Option<CidValue>, f: F) -> Result<()>
    where
        F: Fn(&'_ RelPredicateBuilder) -> Result<()>,
    {
        let builder = RelPredicateBuilder::new(cid);

        f(&builder)?;

        self.rel_predicates
            .borrow_mut()
            .push((id.to_string(), builder));

        Ok(())
    }

    pub fn except<T>(&self, id: &str, bindings: T) -> Result<()>
    where
        T: AtomBindings,
    {
        let builder = NegationBuilder::default();

        bindings.bind(&mut builder.bindings.borrow_mut());

        self.negations.borrow_mut().push((id.to_string(), builder));

        Ok(())
    }

    pub fn build_except<F>(&self, id: &str, f: F) -> Result<()>
    where
        F: Fn(&'_ NegationBuilder) -> Result<()>,
    {
        let builder = NegationBuilder::default();

        f(&builder)?;

        self.negations.borrow_mut().push((id.to_string(), builder));

        Ok(())
    }

    pub fn predicate<I, Pred>(&self, pred: Pred) -> Result<()>
    where
        I: Args,
        Pred: PredicateWhere<I>,
        Pred::Predicate: PredicateWrapper,
    {
        let args = pred.as_args();
        let wrapper = Arc::new(pred.into_predicate());

        self.var_predicates.borrow_mut().push((args, wrapper));

        Ok(())
    }

    pub fn group_by<GroupBy, Agg, I, O>(
        &self,
        target: TypedVar<O>,
        id: &str,
        group_by: GroupBy,
        agg: Agg,
    ) -> Result<()>
    where
        GroupBy: AtomBindings,
        Agg: AggregateGroupBy<I, O>,
        I: Args,
        O: AggAcc,
        Agg::Aggregate: AggregateWrapper + 'static,
    {
        let wrapper = Arc::new(Agg::Aggregate::default());
        let mut builder = AggregationBuilder::new(target.into(), wrapper);

        for var in agg.as_args() {
            builder.vars.push(var);
        }

        group_by.bind(&mut builder.bindings);

        self.aggregations
            .borrow_mut()
            .push((id.to_string(), builder));

        Ok(())
    }
}

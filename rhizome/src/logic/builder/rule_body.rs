use anyhow::Result;
use num_traits::{One, Zero};
use std::{cell::RefCell, collections::HashMap, fmt::Debug, ops::Add, rc::Rc, sync::Arc};

use crate::{
    error::{error, Error},
    id::{LinkId, VarId},
    kernel,
    logic::{
        ast::{BodyTerm, CidValue, Declaration, GetLink, VarPredicate},
        AggregationClosure, VarClosure,
    },
    types::{ColType, FromType, Type},
    value::Val,
    var::{TypedVar, Var},
};

use super::{
    aggregation::AggregationBuilder, atom_bindings::AtomBindings, negation::NegationBuilder,
    rel_predicate::RelPredicateBuilder, typed_vars_tuple::TypedVars,
};

type RelPredicates = Vec<(String, RelPredicateBuilder)>;
type Negations = Vec<(String, NegationBuilder)>;
type GetLinks = Vec<(CidValue, LinkId, CidValue)>;
type VarPredicates = Vec<(Vec<Var>, Arc<dyn VarClosure>)>;
type Aggregations = Vec<(String, AggregationBuilder)>;
type Relations = HashMap<String, Arc<Declaration>>;

pub struct RuleBodyBuilder {
    rel_predicates: RefCell<RelPredicates>,
    negations: RefCell<Negations>,
    get_links: RefCell<GetLinks>,
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
            get_links: RefCell::default(),
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

        for (cid, link_id, value) in self.get_links.into_inner() {
            if let CidValue::Var(var) = cid {
                if var.typ().unify(&ColType::Type(Type::Cid)).is_err() {
                    return error(Error::VarTypeConflict(var, Type::Cid));
                }

                if let Some(bound_type) = bound_vars.insert(var.id(), ColType::Type(Type::Cid)) {
                    if bound_type.unify(&ColType::Type(Type::Cid)).is_err() {
                        return error(Error::VarTypeConflict(var, Type::Cid));
                    }
                } else {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            if let CidValue::Var(var) = value {
                if var.typ().unify(&ColType::Type(Type::Cid)).is_err() {
                    return error(Error::VarTypeConflict(var, Type::Cid));
                }

                if let Some(bound_type) = bound_vars.insert(var.id(), ColType::Type(Type::Cid)) {
                    if bound_type.unify(&ColType::Type(Type::Cid)).is_err() {
                        return error(Error::VarTypeConflict(var, Type::Cid));
                    }
                }
            }

            let term = BodyTerm::GetLink(GetLink::new(cid, vec![(link_id, value)])?);

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

    pub fn get_link<C, L, V>(&self, cid: C, link_id: L, value: V) -> Result<()>
    where
        C: Into<CidValue>,
        L: AsRef<str>,
        V: Into<CidValue>,
    {
        let cid = cid.into();
        let link_id = LinkId::new(link_id);
        let value = value.into();

        self.get_links.borrow_mut().push((cid, link_id, value));

        Ok(())
    }

    pub fn predicate<Vars, F>(&self, vars: Vars, f: F) -> Result<()>
    where
        Vars: TypedVars + Send + Sync + 'static,
        F: Fn(Vars::Args) -> bool + Send + Sync + 'static,
    {
        let vars_vec = vars.vars();

        let f: Arc<dyn VarClosure> = Arc::new(move |bindings| {
            let vals = vars.args(bindings)?;

            Ok(f(vals))
        });

        self.var_predicates.borrow_mut().push((vars_vec, f));

        Ok(())
    }

    pub fn fold<Target, Vars, Args, F>(
        &self,
        target: TypedVar<Target>,
        vars: Vars,
        id: &str,
        args: Args,
        init: Target,
        f: F,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val>,
        Type: FromType<Target>,
        Vars: TypedVars + Send + Sync + 'static,
        Args: AtomBindings,
        F: Fn(Target, Vars::Args) -> Target + Send + Sync + 'static,
    {
        self.do_fold(target, vars, id, args, Some(init), f)
    }

    pub fn reduce<Target, Vars, Args, F>(
        &self,
        target: TypedVar<Target>,
        vars: Vars,
        id: &str,
        args: Args,
        f: F,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val>,
        Type: FromType<Target>,
        Vars: TypedVars + Send + Sync + 'static,
        Args: AtomBindings,
        F: Fn(Target, Vars::Args) -> Target + Send + Sync + 'static,
    {
        self.do_fold(target, vars, id, args, None, f)
    }

    // TODO: Find a good way of dynamically registering these
    // https://github.com/RhizomeDB/rs-rhizome/issues/39
    pub fn count<Target, Args>(&self, target: TypedVar<Target>, id: &str, args: Args) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val> + Add + One + Zero + 'static,
        Type: FromType<Target>,
        Args: AtomBindings,
    {
        self.fold(target, (), id, args, Zero::zero(), kernel::math::count)
    }

    pub fn sum<Target, Vars, Args>(
        &self,
        target: TypedVar<Target>,
        var: Vars,
        id: &str,
        args: Args,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val> + Add + Zero + 'static,
        Type: FromType<Target>,
        Vars: TypedVars<Args = Target> + Send + Sync + 'static,
        Args: AtomBindings,
    {
        self.fold(target, var, id, args, Zero::zero(), kernel::math::sum)
    }

    pub fn min<Target, Vars, Args>(
        &self,
        target: TypedVar<Target>,
        var: Vars,
        id: &str,
        args: Args,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val> + Ord + 'static,
        Type: FromType<Target>,
        Vars: TypedVars<Args = Target> + Send + Sync + 'static,
        Args: AtomBindings,
    {
        self.reduce(target, var, id, args, kernel::math::min)
    }

    pub fn max<Target, Vars, Args>(
        &self,
        target: TypedVar<Target>,
        var: Vars,
        id: &str,
        args: Args,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val> + Ord + 'static,
        Type: FromType<Target>,
        Vars: TypedVars<Args = Target> + Send + Sync + 'static,
        Args: AtomBindings,
    {
        self.reduce(target, var, id, args, kernel::math::max)
    }

    fn do_fold<Target, Vars, Args, F>(
        &self,
        target: TypedVar<Target>,
        vars: Vars,
        id: &str,
        args: Args,
        init: Option<Target>,
        f: F,
    ) -> Result<()>
    where
        Val: TryInto<Target, Error = ()>,
        Target: Into<Val>,
        Type: FromType<Target>,
        Vars: TypedVars + Send + Sync + 'static,
        Args: AtomBindings,
        F: Fn(Target, Vars::Args) -> Target + Send + Sync + 'static,
    {
        let vars_vec = vars.vars();

        let f: Arc<dyn AggregationClosure> = Arc::new(move |acc, bindings| {
            let acc = acc.try_into()?;
            let vals = vars.args(bindings)?;

            Ok(f(acc, vals).into())
        });

        let mut builder = AggregationBuilder::new(init.map(Into::into), target.into(), f);

        for var in vars_vec {
            builder.vars.push(var);
        }

        args.bind(&mut builder.bindings);

        self.aggregations
            .borrow_mut()
            .push((id.to_string(), builder));

        Ok(())
    }
}

use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    error::{error, Error},
    id::LinkId,
    logic::{
        ast::{BodyTerm, CidValue, Declaration, GetLink, VarPredicate},
        ReduceClosure, VarClosure,
    },
    types::{FromType, Type},
    value::Val,
    var::{TypedVar, Var},
};

use super::{
    atom_args::AtomArgs,
    negation::NegationBuilder,
    reduce::ReduceBuilder,
    rel_predicate::RelPredicateBuilder,
    typed_vars_tuple::{TypedVarsTuple, VarRefTuple},
};

pub struct RuleBodyBuilder<'a> {
    rel_predicates: Vec<(String, RelPredicateBuilder)>,
    negations: Vec<(String, NegationBuilder)>,
    get_links: Vec<(CidValue, LinkId, CidValue)>,
    var_predicates: Vec<(Vec<Var>, Arc<dyn VarClosure>)>,
    reduces: Vec<(String, ReduceBuilder)>,
    relations: &'a HashMap<String, Arc<Declaration>>,
}

impl<'a> Debug for RuleBodyBuilder<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleBodyBuilder").finish()
    }
}

impl<'a> RuleBodyBuilder<'a> {
    pub fn new(relations: &'a HashMap<String, Arc<Declaration>>) -> Self {
        Self {
            rel_predicates: Vec::default(),
            negations: Vec::default(),
            get_links: Vec::default(),
            var_predicates: Vec::default(),
            reduces: Vec::default(),
            relations,
        }
    }

    pub fn finalize(self, bound_vars: &mut HashMap<Var, Type>) -> Result<Vec<BodyTerm>> {
        let mut body_terms = Vec::default();

        for (id, builder) in self.rel_predicates {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let predicate = builder.finalize(Arc::clone(declaration), bound_vars)?;
            let term = BodyTerm::RelPredicate(predicate);

            body_terms.push(term);
        }

        for (cid, link_id, value) in self.get_links {
            if let CidValue::Var(var) = cid {
                if var.typ() != Type::Cid {
                    return error(Error::VarTypeConflict(var, Type::Cid));
                }

                if let Some(bound_type) = bound_vars.insert(var, Type::Cid) {
                    if bound_type != Type::Cid {
                        return error(Error::VarTypeConflict(var, Type::Cid));
                    }
                } else {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            if let CidValue::Var(var) = value {
                if var.typ() != Type::Cid {
                    return error(Error::VarTypeConflict(var, Type::Cid));
                }

                if let Some(bound_type) = bound_vars.insert(var, Type::Cid) {
                    if bound_type != Type::Cid {
                        return error(Error::VarTypeConflict(var, Type::Cid));
                    }
                }
            }

            let term = BodyTerm::GetLink(GetLink::new(cid, vec![(link_id, value)]));

            body_terms.push(term);
        }

        for (vars, f) in self.var_predicates {
            for var in &vars {
                if !bound_vars.contains_key(var) {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            let term = BodyTerm::VarPredicate(VarPredicate::new(vars, f));

            body_terms.push(term);
        }

        for (id, builder) in self.negations {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let negation = builder.finalize(Arc::clone(declaration))?;

            for var in negation.vars() {
                if !bound_vars.contains_key(var) {
                    return error(Error::ClauseNotDomainIndependent(var.id()));
                }
            }

            let term = BodyTerm::Negation(negation);

            body_terms.push(term);
        }

        for (id, builder) in self.reduces {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let reduce = builder.finalize(Arc::clone(declaration), bound_vars)?;
            let term = BodyTerm::Reduce(reduce);

            body_terms.push(term);
        }

        Ok(body_terms)
    }

    pub fn search<T, A>(mut self, id: &str, t: T) -> Self
    where
        T: AtomArgs<A>,
    {
        let mut builder = RelPredicateBuilder::new();

        for (col_id, col_val) in T::into_cols(t) {
            builder.bindings.push((col_id, col_val));
        }

        self.rel_predicates.push((id.to_string(), builder));

        self
    }

    pub fn build_search<F>(mut self, id: &str, f: F) -> Self
    where
        F: Fn(RelPredicateBuilder) -> RelPredicateBuilder,
    {
        let builder = RelPredicateBuilder::new();
        let builder = f(builder);

        self.rel_predicates.push((id.to_string(), builder));

        self
    }

    pub fn except<T, A>(mut self, id: &str, t: T) -> Self
    where
        T: AtomArgs<A>,
    {
        let mut builder = NegationBuilder::default();

        for (col_id, col_val) in T::into_cols(t) {
            builder.bindings.push((col_id, col_val));
        }

        self.negations.push((id.to_string(), builder));

        self
    }

    pub fn build_except<F>(mut self, id: &str, f: F) -> Self
    where
        F: Fn(NegationBuilder) -> NegationBuilder,
    {
        let builder = NegationBuilder::default();
        let builder = f(builder);

        self.negations.push((id.to_string(), builder));

        self
    }

    pub fn get_link<C, L, V>(mut self, cid: C, link_id: L, value: V) -> Self
    where
        C: Into<CidValue>,
        L: AsRef<str>,
        V: Into<CidValue>,
    {
        let cid = cid.into();
        let link_id = LinkId::new(link_id);
        let value = value.into();

        self.get_links.push((cid, link_id, value));

        self
    }

    pub fn predicate<VarsRef, VarArgs, F>(mut self, vars: VarsRef, f: F) -> Self
    where
        VarsRef: VarRefTuple<Val, Target = VarArgs>,
        VarArgs: TypedVarsTuple<Val> + Send + Sync + 'static,
        F: Fn(VarArgs::Output) -> bool + Send + Sync + 'static,
    {
        let owned = vars.deref();
        let vars_vec = owned.vars();

        let f: Arc<dyn VarClosure> = Arc::new(move |bindings| {
            let args = owned.args(bindings);

            f(args)
        });

        self.var_predicates.push((vars_vec, f));

        self
    }

    pub fn reduce<Target, VarsRef, VarArgs, Args, Arg, F>(
        mut self,
        target: &TypedVar<Target>,
        vars: VarsRef,
        id: &str,
        args: Args,
        init: Target,
        f: F,
    ) -> Self
    where
        Val: TryInto<Target, Error = &'static str>,
        Target: Into<Val> + Clone,
        Type: FromType<Target>,
        VarsRef: VarRefTuple<Val, Target = VarArgs>,
        VarArgs: TypedVarsTuple<Val> + Send + Sync + 'static,
        Args: AtomArgs<Arg>,
        F: Fn(Target, VarArgs::Output) -> Target + Send + Sync + 'static,
    {
        let owned = vars.deref();
        let vars_vec = owned.vars();

        let f: Arc<dyn ReduceClosure> = Arc::new(move |acc, bindings| {
            let acc = acc.try_into().unwrap();
            let args = owned.args(bindings);

            f(acc, args).into()
        });

        let mut builder = ReduceBuilder::new(init.into(), (*target).clone().into(), f);

        for var in vars_vec {
            builder.vars.push(var);
        }

        for (col_id, col_val) in Args::into_cols(args) {
            builder.bindings.push((col_id, col_val));
        }

        self.reduces.push((id.to_string(), builder));

        self
    }
}

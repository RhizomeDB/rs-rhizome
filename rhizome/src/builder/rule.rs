use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColId, LinkId},
    logic::ast::{BodyTerm, CidValue, ColVal, Declaration, GetLink, Var},
    types::Type,
    value::Val,
};

use super::{
    atom_args::{AtomArg, AtomArgs},
    negation::NegationBuilder,
    predicate::PredicateBuilder,
};

#[derive(Debug)]
pub struct RuleHeadBuilder<'a> {
    relation: &'a Declaration,
    pub(super) bindings: Vec<(ColId, ColVal)>,
}

impl<'a> RuleHeadBuilder<'a> {
    pub fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            bindings: Vec::default(),
        }
    }

    pub fn finalize(self, bound_vars: &HashMap<Var, Type>) -> Result<HashMap<ColId, ColVal>> {
        let mut cols = HashMap::default();

        for (col_id, col_val) in self.bindings {
            let Some(col) = self.relation.schema().get_col(&col_id) else {
                return error(Error::UnrecognizedColumnBinding(self.relation.id(), col_id));
            };

            if cols.contains_key(&col_id) {
                return error(Error::ConflictingColumnBinding(self.relation.id(), col_id));
            }

            match &col_val {
                ColVal::Lit(val) => {
                    if col.col_type().check(val).is_err() {
                        return error(Error::ColumnValueTypeConflict(
                            self.relation.id(),
                            col_id,
                            col_val,
                            *col.col_type(),
                        ));
                    }
                }
                ColVal::Binding(var) => {
                    if col.col_type().downcast(&var.typ()).is_none() {
                        return error(Error::ColumnValueTypeConflict(
                            self.relation.id(),
                            col_id,
                            ColVal::Binding(*var),
                            *col.col_type(),
                        ));
                    }
                }
            }

            cols.insert(col_id, col_val);
        }

        for col_id in self.relation.schema().cols().keys() {
            if !cols.contains_key(col_id) {
                return error(Error::ColumnMissing(self.relation.id(), *col_id));
            }
        }

        for (col_id, val) in &cols {
            if let ColVal::Binding(var) = val {
                if !bound_vars.contains_key(var) {
                    return error(Error::ClauseNotRangeRestricted(*col_id, var.id()));
                }
            }
        }

        Ok(cols)
    }

    pub fn set<S, T>(mut self, id: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Val>,
    {
        let id = ColId::new(id);
        let value = value.into();

        self.bindings.push((id, ColVal::Lit(value)));

        self
    }

    pub fn bind<T, A>(mut self, bindings: T) -> Self
    where
        T: AtomArgs<A>,
    {
        for (id, value) in T::into_cols(bindings) {
            self.bindings.push((id, value));
        }

        self
    }

    pub fn bind_one<T, A>(mut self, binding: T) -> Self
    where
        T: AtomArg<A>,
    {
        let (id, value) = binding.into_col();

        self.bindings.push((id, value));

        self
    }
}

pub struct RuleBodyBuilder<'a> {
    predicates: Vec<(String, PredicateBuilder)>,
    negations: Vec<(String, NegationBuilder)>,
    get_links: Vec<(CidValue, LinkId, CidValue)>,
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
            predicates: Vec::default(),
            negations: Vec::default(),
            get_links: Vec::default(),
            relations,
        }
    }

    pub fn finalize(self, bound_vars: &mut HashMap<Var, Type>) -> Result<Vec<BodyTerm>> {
        let mut body_terms = Vec::default();

        for (id, builder) in self.predicates {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let predicate = builder.finalize(Arc::clone(declaration), bound_vars)?;
            let term = BodyTerm::Predicate(predicate);

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

        Ok(body_terms)
    }

    pub fn search<T, A>(mut self, id: &str, t: T) -> Self
    where
        T: AtomArgs<A>,
    {
        let mut builder = PredicateBuilder::new();

        for (col_id, col_val) in T::into_cols(t) {
            builder.bindings.push((col_id, col_val));
        }

        self.predicates.push((id.to_string(), builder));

        self
    }

    pub fn build_search<F>(mut self, id: &str, f: F) -> Self
    where
        F: Fn(PredicateBuilder) -> PredicateBuilder,
    {
        let builder = PredicateBuilder::new();
        let builder = f(builder);

        self.predicates.push((id.to_string(), builder));

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
}

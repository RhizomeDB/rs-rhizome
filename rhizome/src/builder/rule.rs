use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, LinkId, VarId},
    logic::ast::{BodyTerm, CidValue, ColumnValue, Declaration, GetLink},
    types::{ColumnType, Type},
    value::Value,
};

use super::{
    atom_args::{AtomArg, AtomArgs},
    negation::NegationBuilder,
    predicate::PredicateBuilder,
};

#[derive(Debug)]
pub struct RuleHeadBuilder<'a> {
    relation: &'a Declaration,
    pub(super) bindings: Vec<(ColumnId, ColumnValue)>,
}

impl<'a> RuleHeadBuilder<'a> {
    pub fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            bindings: Vec::default(),
        }
    }

    pub fn finalize(
        self,
        bound_vars: &HashMap<VarId, Type>,
    ) -> Result<HashMap<ColumnId, ColumnValue>> {
        let mut columns = HashMap::default();

        for (column_id, column_value) in self.bindings {
            let Some(column) = self.relation.schema().get_column(&column_id) else {
                return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
            };

            if columns.contains_key(&column_id) {
                return error(Error::ConflictingColumnBinding(column_id));
            }

            match &column_value {
                ColumnValue::Literal(value) => column.column_type().check(value)?,
                ColumnValue::Binding(var) => {
                    if column.column_type().downcast(&var.typ()).is_none() {
                        return error(Error::VariableTypeConflict(
                            var.id(),
                            *column.column_type(),
                            var.typ(),
                        ));
                    }
                }
            }

            columns.insert(column_id, column_value);
        }

        for column_id in self.relation.schema().columns().keys() {
            if !columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        for (column_id, value) in &columns {
            if let ColumnValue::Binding(var) = value {
                if !bound_vars.contains_key(&var.id()) {
                    return error(Error::ClauseNotRangeRestricted(*column_id, var.id()));
                }
            }
        }

        Ok(columns)
    }

    pub fn set<S, T>(mut self, id: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let id = ColumnId::new(id);
        let value = value.into();

        self.bindings.push((id, ColumnValue::Literal(value)));

        self
    }

    pub fn bind<T, A>(mut self, bindings: T) -> Self
    where
        T: AtomArgs<A>,
    {
        for (id, value) in T::into_columns(bindings) {
            self.bindings.push((id, value));
        }

        self
    }

    pub fn bind_one<T, A>(mut self, binding: T) -> Self
    where
        T: AtomArg<A>,
    {
        let (id, value) = binding.into_column();

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

    pub fn finalize(self, bound_vars: &mut HashMap<VarId, Type>) -> Result<Vec<BodyTerm>> {
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
                if let Some(bound_type) = bound_vars.insert(var.id(), Type::Cid) {
                    if bound_type != Type::Cid {
                        return error(Error::VariableTypeConflict(
                            var.id(),
                            ColumnType::Type(Type::Cid),
                            bound_type,
                        ));
                    }
                }
            }

            if let CidValue::Var(var) = value {
                if let Some(bound_type) = bound_vars.insert(var.id(), Type::Cid) {
                    if bound_type != Type::Cid {
                        return error(Error::VariableTypeConflict(
                            var.id(),
                            ColumnType::Type(Type::Cid),
                            bound_type,
                        ));
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

            let negation = builder.finalize(Arc::clone(declaration), bound_vars)?;

            for var_id in negation.variables() {
                if !bound_vars.contains_key(&var_id) {
                    return error(Error::ClauseNotDomainIndependent(var_id));
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

        for (column_id, column_value) in T::into_columns(t) {
            builder.bindings.push((column_id, column_value));
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

        for (column_id, column_value) in T::into_columns(t) {
            builder.bindings.push((column_id, column_value));
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

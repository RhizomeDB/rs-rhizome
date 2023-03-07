use anyhow::Result;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    error::{error, Error},
    id::{ColumnId, LinkId, VarId},
    logic::ast::{BodyTerm, CidValue, ColumnValue, Declaration, GetLink, Var},
    types::{ColumnType, Type},
    value::Value,
};

use super::{negation::NegationBuilder, predicate::PredicateBuilder};

#[derive(Debug)]
pub struct RuleHeadBuilder<'a> {
    relation: &'a Declaration,
    bindings: Vec<(ColumnId, ColumnValue)>,
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
        bound_vars: &HashMap<VarId, ColumnType>,
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
                ColumnValue::Literal(value) => column.column_type().check(&value)?,
                ColumnValue::Binding(var_id) => {
                    if let Some(bound_type) = bound_vars.get(&var_id) {
                        if bound_type != column.column_type() {
                            return error(Error::VariableTypeConflict(
                                *var_id,
                                *column.column_type(),
                                *bound_type,
                            ));
                        }
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
            if let ColumnValue::Binding(var_id) = value {
                if !bound_vars.contains_key(&var_id) {
                    return error(Error::ClauseNotRangeRestricted(*column_id, *var_id));
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

    pub fn bind<S>(mut self, column_id: S, var: &Var) -> Self
    where
        S: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);

        self.bindings
            .push((column_id, ColumnValue::Binding(var.id())));

        self
    }
}

pub struct RuleBodyBuilder<'a> {
    predicates: Vec<(String, Box<dyn Fn(PredicateBuilder) -> PredicateBuilder>)>,
    negations: Vec<(String, Box<dyn Fn(NegationBuilder) -> NegationBuilder>)>,
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

    pub fn finalize(self, bound_vars: &mut HashMap<VarId, ColumnType>) -> Result<Vec<BodyTerm>> {
        let mut body_terms = Vec::default();

        for (id, predicate_f) in self.predicates {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let builder = PredicateBuilder::new(Arc::clone(declaration));
            let builder = predicate_f(builder);
            let predicate = builder.finalize(bound_vars)?;
            let term = BodyTerm::Predicate(predicate);

            body_terms.push(term);
        }

        for (cid, link_id, value) in self.get_links {
            if let CidValue::Var(var_id) = cid {
                if let Some(bound_type) = bound_vars.insert(var_id, ColumnType::Type(Type::Cid)) {
                    if bound_type != ColumnType::Type(Type::Cid) {
                        return error(Error::VariableTypeConflict(
                            var_id,
                            ColumnType::Type(Type::Cid),
                            bound_type,
                        ));
                    }
                }
            }

            if let CidValue::Var(var_id) = value {
                if let Some(bound_type) = bound_vars.insert(var_id, ColumnType::Type(Type::Cid)) {
                    if bound_type != ColumnType::Type(Type::Cid) {
                        return error(Error::VariableTypeConflict(
                            var_id,
                            ColumnType::Type(Type::Cid),
                            bound_type,
                        ));
                    }
                }
            }

            let term = BodyTerm::GetLink(GetLink::new(cid, vec![(link_id, value)]));

            body_terms.push(term);
        }

        for (id, negation_f) in self.negations {
            let Some(declaration) = self.relations.get(&id) else {
                    return error(Error::UnrecognizedRelation(id));
                };

            let builder = NegationBuilder::new(Arc::clone(declaration));
            let builder = negation_f(builder);
            let negation = builder.finalize(bound_vars)?;

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

    pub fn search<F>(mut self, id: &str, f: F) -> Self
    where
        F: Fn(PredicateBuilder) -> PredicateBuilder + 'static,
    {
        self.predicates.push((id.to_string(), Box::new(f)));

        self
    }

    pub fn except<F>(mut self, id: &str, f: F) -> Self
    where
        F: Fn(NegationBuilder) -> NegationBuilder + 'static,
    {
        self.negations.push((id.to_string(), Box::new(f)));

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

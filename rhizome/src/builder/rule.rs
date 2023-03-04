use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    error::{error, Error},
    id::{ColumnId, VarId},
    logic::ast::{BodyTerm, ColumnValue, Declaration},
    types::ColumnType,
    value::Value,
};

use super::{negation::NegationBuilder, predicate::PredicateBuilder};

#[derive(Debug)]
pub struct RuleHeadBuilder<'a, 'b> {
    relation: &'a Declaration,
    columns: HashMap<ColumnId, ColumnValue>,
    bound_vars: &'b mut HashMap<VarId, ColumnType>,
}

impl<'a, 'b> RuleHeadBuilder<'a, 'b> {
    fn new(relation: &'a Declaration, bound_vars: &'b mut HashMap<VarId, ColumnType>) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
            bound_vars,
        }
    }

    // TODO: make AST node for RuleHead?
    pub fn build<F>(
        relation: &'a Declaration,
        bound_vars: &'b mut HashMap<VarId, ColumnType>,
        f: F,
    ) -> Result<HashMap<ColumnId, ColumnValue>>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        let builder = f(Self::new(relation, bound_vars))?;
        let result = builder.finalize()?;

        Ok(result)
    }

    pub fn finalize(self) -> Result<HashMap<ColumnId, ColumnValue>> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        for (column_id, value) in &self.columns {
            if let ColumnValue::Binding(variable_id) = value {
                if !self.bound_vars.contains_key(variable_id) {
                    return error(Error::ClauseNotRangeRestricted(*column_id, *variable_id));
                }
            }
        }

        Ok(self.columns)
    }

    pub fn set<S, T>(mut self, id: S, value: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let id = ColumnId::new(id);
        let value = value.into();

        let Some(column) = self.relation.schema().get_column(&id) else {
            return error(Error::UnrecognizedColumnBinding(id, self.relation.id()));
        };

        if self.columns.contains_key(&id) {
            return error(Error::ConflictingColumnBinding(id));
        }

        column.column_type().check(&value)?;

        self.columns.insert(id, ColumnValue::Literal(value));

        Ok(self)
    }

    pub fn bind<S, T>(mut self, column_id: S, variable_id: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);
        let variable_id = VarId::new(variable_id);

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(
                column_id,
                self.relation.id(),
            ));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        if let Some(bound_type) = self.bound_vars.get(&variable_id) {
            if bound_type != column.column_type() {
                return error(Error::VariableTypeConflict(
                    variable_id,
                    *column.column_type(),
                    *bound_type,
                ));
            }
        } else {
            self.bound_vars.insert(variable_id, *column.column_type());
        }

        self.columns
            .insert(column_id, ColumnValue::Binding(variable_id));

        Ok(self)
    }
}

#[derive(Debug)]
pub struct RuleBodyBuilder<'a, 'b> {
    body_terms: Vec<BodyTerm>,
    relations: &'a HashMap<String, Arc<Declaration>>,
    bound_vars: &'b mut HashMap<VarId, ColumnType>,
    negative_variables: HashSet<VarId>,
}

impl<'a, 'b> RuleBodyBuilder<'a, 'b> {
    fn new(
        relations: &'a HashMap<String, Arc<Declaration>>,
        bound_vars: &'b mut HashMap<VarId, ColumnType>,
    ) -> Self {
        Self {
            body_terms: Vec::default(),
            relations,
            bound_vars,
            negative_variables: HashSet::default(),
        }
    }

    pub fn build<F>(
        relations: &'a HashMap<String, Arc<Declaration>>,
        bound_vars: &'b mut HashMap<VarId, ColumnType>,
        f: F,
    ) -> Result<Vec<BodyTerm>>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relations, bound_vars))?.finalize()
    }

    pub fn finalize(self) -> Result<Vec<BodyTerm>> {
        for variable_id in &self.negative_variables {
            if !self.bound_vars.contains_key(variable_id) {
                return error(Error::ClauseNotDomainIndependent(*variable_id));
            }
        }

        Ok(self.body_terms)
    }

    pub fn search<S, F>(mut self, id: S, f: F) -> Result<Self>
    where
        S: AsRef<str>,
        F: for<'c> FnOnce(PredicateBuilder<'c>) -> Result<PredicateBuilder<'c>>,
    {
        let Some(declaration) = self.relations.get(id.as_ref()) else {
                return error(Error::UnrecognizedRelation(id.as_ref().to_string()));
            };

        let predicate = PredicateBuilder::build(Arc::clone(declaration), self.bound_vars, f)?;
        let term = BodyTerm::Predicate(predicate);

        self.body_terms.push(term);

        Ok(self)
    }

    pub fn except<S, F>(mut self, id: S, f: F) -> Result<Self>
    where
        S: AsRef<str>,
        F: FnOnce(NegationBuilder) -> Result<NegationBuilder>,
    {
        let Some(declaration) = self.relations.get(id.as_ref()) else {
            return error(Error::UnrecognizedRelation(id.as_ref().to_string()));
        };

        let negation = NegationBuilder::build(Arc::clone(declaration), f)?;

        for variable in negation.variables() {
            self.negative_variables.insert(variable);
        }

        let term = BodyTerm::Negation(negation);

        self.body_terms.push(term);

        Ok(self)
    }
}
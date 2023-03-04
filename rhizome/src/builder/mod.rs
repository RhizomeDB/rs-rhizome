use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use crate::{
    error::{error, Error},
    id::{ColumnId, RelationId, VarId},
    relation::{RelationSource, EDB, IDB},
    types::{ColumnType, FromType, Type},
    value::Value,
};

use crate::logic::ast::{
    BodyTerm, Clause, Column, ColumnValue, Declaration, Fact, InnerDeclaration, Negation,
    Predicate, Program, Rule, Schema,
};

#[derive(Debug, Default)]
pub struct ProgramBuilder {
    relations: HashMap<String, Arc<Declaration>>,
    clauses: Vec<Clause>,
}

impl ProgramBuilder {
    pub fn build<F>(f: F) -> Result<Program>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        let mut builder = Self::default();

        f(&mut builder)?;

        builder.finalize()
    }

    pub fn finalize(self) -> Result<Program> {
        let declarations = self.relations.into_values().collect();
        let program = Program::new(declarations, self.clauses);

        Ok(program)
    }

    pub fn input<F>(&mut self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(DeclarationBuilder<EDB>) -> Result<DeclarationBuilder<EDB>>,
    {
        if let Some(relation) = self.relations.get(&id.to_owned()) {
            return error(Error::ConflictingRelationDeclaration(relation.id()));
        }

        let rel_id = RelationId::new(id);
        let relation = DeclarationBuilder::build(rel_id, f)?;
        let relation = Declaration::EDB(Arc::new(relation));

        self.relations.insert(id.to_owned(), Arc::new(relation));

        Ok(())
    }

    pub fn output<F>(&mut self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(DeclarationBuilder<IDB>) -> Result<DeclarationBuilder<IDB>>,
    {
        if let Some(relation) = self.relations.get(&id.to_owned()) {
            return error(Error::ConflictingRelationDeclaration(relation.id()));
        }

        let rel_id = RelationId::new(id);
        let relation = DeclarationBuilder::build(rel_id, f)?;
        let relation = Declaration::IDB(Arc::new(relation));

        self.relations.insert(id.to_owned(), Arc::new(relation));

        Ok(())
    }

    pub fn fact<'b, F>(&'b mut self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(FactBuilder<'b>) -> Result<FactBuilder<'b>>,
    {
        let Some(declaration) = self.relations.get(id) else {
            return error(Error::UnrecognizedRelation(id.to_string()));
        };

        let fact = FactBuilder::build(declaration, f)?;
        let clause = Clause::Fact(fact);

        self.clauses.push(clause);

        Ok(())
    }

    pub fn rule<'a, H, B>(&'a mut self, id: &str, h: H, b: B) -> Result<()>
    where
        H: for<'b> FnOnce(RuleHeadBuilder<'a, 'b>) -> Result<RuleHeadBuilder<'a, 'b>>,
        B: for<'b> FnOnce(RuleBodyBuilder<'a, 'b>) -> Result<RuleBodyBuilder<'a, 'b>>,
    {
        let Some(declaration) = self.relations.get(id) else {
                return error(Error::UnrecognizedRelation(id.to_string()));
            };

        let mut bound_vars = HashMap::default();
        let body = RuleBodyBuilder::build(&self.relations, &mut bound_vars, b)?;
        let head = RuleHeadBuilder::build(declaration, &mut bound_vars, h)?;

        match &**declaration {
            Declaration::EDB(inner) => error(Error::ClauseHeadEDB(inner.id())),
            Declaration::IDB(inner) => {
                let rule = Rule::new(inner.id(), head, body);
                let clause = Clause::Rule(rule);

                self.clauses.push(clause);

                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct DeclarationBuilder<T> {
    id: RelationId,
    columns: HashMap<ColumnId, Column>,
    _marker: PhantomData<T>,
}

impl<T> DeclarationBuilder<T>
where
    T: RelationSource,
{
    fn new(id: RelationId) -> Self {
        Self {
            id,
            columns: HashMap::default(),
            _marker: PhantomData::default(),
        }
    }

    fn finalize(self) -> Result<InnerDeclaration<T>> {
        let schema = Schema::new(self.columns);
        let declaration = InnerDeclaration::new(self.id, schema);

        Ok(declaration)
    }

    pub fn build<F>(id: RelationId, f: F) -> Result<InnerDeclaration<T>>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(id))?.finalize()
    }

    pub fn column<C>(mut self, id: &str) -> Result<Self>
    where
        Type: FromType<C>,
    {
        let id = ColumnId::new(id);
        let t = ColumnType::new::<C>();
        let column = Column::new(id, t);

        if self.columns.insert(id, column).is_none() {
            Ok(self)
        } else {
            error(Error::DuplicateSchemaAttributeId(id))
        }
    }
}

#[derive(Debug)]
pub struct FactBuilder<'a> {
    relation: &'a Declaration,
    columns: HashMap<ColumnId, Value>,
}

impl<'a> FactBuilder<'a> {
    fn new(relation: &'a Declaration) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
        }
    }

    pub fn build<F>(relation: &'a Declaration, f: F) -> Result<Fact>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation))?.finalize()
    }

    pub fn finalize(self) -> Result<Fact> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        match self.relation {
            Declaration::EDB(inner) => error(Error::ClauseHeadEDB(inner.id())),
            Declaration::IDB(inner) => {
                let fact = Fact::new(inner.id(), self.columns);

                Ok(fact)
            }
        }
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

        self.columns.insert(id, value);

        Ok(self)
    }
}

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

#[derive(Debug)]
pub struct PredicateBuilder<'a> {
    relation: Arc<Declaration>,
    columns: HashMap<ColumnId, ColumnValue>,
    bound_vars: &'a mut HashMap<VarId, ColumnType>,
}

impl<'a> PredicateBuilder<'a> {
    fn new(relation: Arc<Declaration>, bound_vars: &'a mut HashMap<VarId, ColumnType>) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
            bound_vars,
        }
    }

    pub fn build<F>(
        relation: Arc<Declaration>,
        bound_vars: &'a mut HashMap<VarId, ColumnType>,
        f: F,
    ) -> Result<Predicate>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation, bound_vars))?.finalize()
    }

    pub fn finalize(self) -> Result<Predicate> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        let predicate = Predicate::new(self.relation, self.columns);

        Ok(predicate)
    }

    pub fn bind<S, T>(mut self, column_id: S, variable_id: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);
        let variable_id = VarId::new(variable_id);

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()))
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

    pub fn when<S, T>(mut self, column_id: S, value: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let column_id = ColumnId::new(column_id);
        let value = value.into();

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        column.column_type().check(&value)?;

        self.columns.insert(column_id, ColumnValue::Literal(value));

        Ok(self)
    }
}

#[derive(Debug)]
pub struct NegationBuilder {
    relation: Arc<Declaration>,
    columns: HashMap<ColumnId, ColumnValue>,
}

impl NegationBuilder {
    fn new(relation: Arc<Declaration>) -> Self {
        Self {
            relation,
            columns: HashMap::default(),
        }
    }

    pub fn build<F>(relation: Arc<Declaration>, f: F) -> Result<Negation>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        f(Self::new(relation))?.finalize()
    }

    fn finalize(self) -> Result<Negation> {
        for column_id in self.relation.schema().columns().keys() {
            if !self.columns.contains_key(column_id) {
                return error(Error::ColumnMissing(*column_id, self.relation.id()));
            }
        }

        Ok(Negation::new(self.relation, self.columns))
    }

    pub fn bind<S, T>(mut self, column_id: S, variable_id: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let column_id = ColumnId::new(column_id);
        let variable_id = VarId::new(variable_id);

        if !self.relation.schema().has_column(&column_id) {
            return error(Error::UnrecognizedColumnBinding(
                column_id,
                self.relation.id(),
            ));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        self.columns
            .insert(column_id, ColumnValue::Binding(variable_id));

        Ok(self)
    }

    pub fn when<S, T>(mut self, column_id: S, value: T) -> Result<Self>
    where
        S: AsRef<str>,
        T: Into<Value>,
    {
        let column_id = ColumnId::new(column_id);
        let value = value.into();

        let Some(column) = self.relation.schema().get_column(&column_id) else {
            return error(Error::UnrecognizedColumnBinding(column_id, self.relation.id()));
        };

        if self.columns.contains_key(&column_id) {
            return error(Error::ConflictingColumnBinding(column_id));
        }

        column.column_type().check(&value)?;

        self.columns.insert(column_id, ColumnValue::Literal(value));

        Ok(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_builder() -> Result<()> {
        let _p = ProgramBuilder::build(|p| {
            p.input("edge", |h| h.column::<i32>("from")?.column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from")?.column::<i32>("to"))?;

            p.rule(
                "path",
                |h| h.bind("from", "x")?.bind("to", "y"),
                |b| b.search("edge", |s| s.bind("from", "x")?.bind("to", "y")),
            )?;

            p.rule(
                "path",
                |h| h.bind("from", "x")?.bind("to", "z"),
                |b| {
                    b.search("edge", |s| s.bind("from", "x")?.bind("to", "y"))?
                        .search("path", |s| s.bind("from", "y")?.bind("to", "z"))
                },
            )
        })?;

        Ok(())
    }
}

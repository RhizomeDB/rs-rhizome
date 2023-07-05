use anyhow::Result;
use cid::Cid;
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use crate::{
    error::{error, Error},
    id::RelationId,
    logic::ast::{Clause, Declaration, Program, Rule},
    relation::{Bistore, Hexastore, Relation, Source},
    tuple::Tuple,
    types::Any,
};

use super::{
    declaration::DeclarationBuilder, fact::FactBuilder, rule_body::RuleBodyBuilder,
    rule_head::RuleHeadBuilder, rule_vars::RuleVars,
};

type RuleBuilderClosure<'a, T> =
    dyn Fn(&'_ RuleHeadBuilder, &'_ RuleBodyBuilder, T) -> Result<()> + 'a;

#[derive(Debug, Default)]
pub struct ProgramBuilder {
    relations: Rc<RefCell<HashMap<String, Arc<Declaration>>>>,
    clauses: RefCell<Vec<Clause>>,
}

impl ProgramBuilder {
    pub fn build<F>(f: F) -> Result<Program>
    where
        F: FnOnce(Self) -> Result<Self>,
    {
        let builder = Self::default().install_preamble()?;
        let builder = f(builder)?;

        builder.finalize()
    }

    pub fn finalize(self) -> Result<Program> {
        let declarations = self.relations.borrow_mut().values().cloned().collect();
        let program = Program::new(declarations, self.clauses.into_inner());

        Ok(program)
    }

    pub fn input<F>(&self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(DeclarationBuilder) -> DeclarationBuilder,
    {
        self.indexed_input(id, f)
    }

    pub fn indexed_input<R, F>(&self, id: &str, f: F) -> Result<()>
    where
        R: Relation + Default,
        F: FnOnce(DeclarationBuilder<R>) -> DeclarationBuilder<R>,
    {
        if let Some(relation) = self.relations.borrow().get(&id.to_owned()) {
            return error(Error::ConflictingRelationDeclaration(relation.id()));
        }

        let rel_id = RelationId::new(id);
        let relation = DeclarationBuilder::<R>::build(rel_id, Source::Edb, f)?;

        self.relations
            .borrow_mut()
            .insert(id.to_owned(), Arc::new(relation));

        Ok(())
    }

    pub fn output<F>(&self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(DeclarationBuilder) -> DeclarationBuilder,
    {
        self.indexed_output(id, f)
    }

    pub fn indexed_output<R, F>(&self, id: &str, f: F) -> Result<()>
    where
        R: Relation + Default,
        F: FnOnce(DeclarationBuilder<R>) -> DeclarationBuilder<R>,
    {
        if let Some(relation) = self.relations.borrow().get(&id.to_owned()) {
            return error(Error::ConflictingRelationDeclaration(relation.id()));
        }

        let rel_id = RelationId::new(id);
        let relation = DeclarationBuilder::<R>::build(rel_id, Source::Idb, f)?;

        self.relations
            .borrow_mut()
            .insert(id.to_owned(), Arc::new(relation));

        Ok(())
    }

    pub fn fact<F>(&self, id: &str, f: F) -> Result<()>
    where
        F: FnOnce(FactBuilder) -> FactBuilder,
    {
        let Some(declaration) = self.relations.borrow().get(id).cloned() else {
            return error(Error::UnrecognizedRelation(id.to_string()));
        };

        let fact = FactBuilder::build(declaration, f)?;
        let clause = Clause::Fact(fact);

        self.clauses.borrow_mut().push(clause);

        Ok(())
    }

    pub fn rule<T>(&self, id: &str, f: &RuleBuilderClosure<'_, T::Vars>) -> Result<()>
    where
        T: RuleVars,
    {
        let Some(declaration) = self.relations.borrow().get(id).cloned() else {
            return error(Error::UnrecognizedRelation(id.to_string()));
        };

        let mut bound_vars = HashMap::default();
        let head_builder = RuleHeadBuilder::new(Arc::clone(&declaration));
        let body_builder = RuleBodyBuilder::new(Rc::clone(&self.relations));

        f(&head_builder, &body_builder, T::into_vars(0))?;

        let body = body_builder.finalize(&mut bound_vars)?;
        let head = head_builder.finalize(&mut bound_vars)?;

        match declaration.source() {
            Source::Edb => error(Error::ClauseHeadEDB(declaration.id())),
            Source::Idb => {
                let rule = Rule::new(declaration.id(), head, body);
                let clause = Clause::Rule(rule);

                self.clauses.borrow_mut().push(clause);

                Ok(())
            }
        }
    }

    fn install_preamble(self) -> Result<Self> {
        self.indexed_input::<Hexastore<Tuple>, _>("evac", |h| {
            h.column::<Any>("entity")
                .column::<Any>("attribute")
                .column::<Any>("value")
        })?;

        self.indexed_input::<Bistore<Tuple>, _>("links", |h| {
            h.column::<Cid>("from").column::<Cid>("to")
        })?;

        Ok(self)
    }
}

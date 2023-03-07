use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, sync::Arc};

use crate::{
    error::{error, Error},
    id::{RelationId, VarId},
    logic::ast::{Clause, Declaration, Program, Rule},
    relation::{EDB, IDB},
    types::{FromType, Type},
};

use super::{
    declaration::DeclarationBuilder,
    fact::FactBuilder,
    rule::{RuleBodyBuilder, RuleHeadBuilder},
};

pub trait RuleVars {
    type Vars: Copy;

    fn into_vars() -> Self::Vars;
}

impl RuleVars for () {
    type Vars = ();

    fn into_vars() -> Self::Vars {
        ()
    }
}

impl<V0> RuleVars for (V0,)
where
    Type: FromType<V0>,
{
    type Vars = (VarId,);

    fn into_vars() -> Self::Vars {
        (VarId::new("x0"),)
    }
}

impl<V0, V1> RuleVars for (V0, V1)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    type Vars = (VarId, VarId);

    fn into_vars() -> Self::Vars {
        (VarId::new("x0"), VarId::new("x1"))
    }
}

impl<V0, V1, V2> RuleVars for (V0, V1, V2)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    type Vars = (VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (VarId::new("x0"), VarId::new("x1"), VarId::new("x2"))
    }
}

impl<V0, V1, V2, V3> RuleVars for (V0, V1, V2, V3)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    type Vars = (VarId, VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
        )
    }
}

impl<V0, V1, V2, V3, V4> RuleVars for (V0, V1, V2, V3, V4)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
{
    type Vars = (VarId, VarId, VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5> RuleVars for (V0, V1, V2, V3, V4, V5)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
{
    type Vars = (VarId, VarId, VarId, VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
            VarId::new("x5"),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6> RuleVars for (V0, V1, V2, V3, V4, V5, V6)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
{
    type Vars = (VarId, VarId, VarId, VarId, VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
            VarId::new("x5"),
            VarId::new("x6"),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
{
    type Vars = (VarId, VarId, VarId, VarId, VarId, VarId, VarId, VarId);

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
            VarId::new("x5"),
            VarId::new("x6"),
            VarId::new("x7"),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7, V8> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
    Type: FromType<V8>,
{
    type Vars = (
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
    );

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
            VarId::new("x5"),
            VarId::new("x6"),
            VarId::new("x7"),
            VarId::new("x8"),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7, V8, V9> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8, V9)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
    Type: FromType<V8>,
    Type: FromType<V9>,
{
    type Vars = (
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
        VarId,
    );

    fn into_vars() -> Self::Vars {
        (
            VarId::new("x0"),
            VarId::new("x1"),
            VarId::new("x2"),
            VarId::new("x3"),
            VarId::new("x4"),
            VarId::new("x5"),
            VarId::new("x6"),
            VarId::new("x7"),
            VarId::new("x8"),
            VarId::new("x9"),
        )
    }
}

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

    pub fn rule<'a, 'b, T>(
        &'a mut self,
        id: &str,
        f: &'b dyn for<'c> Fn(
            RuleHeadBuilder<'a, 'c>,
            RuleBodyBuilder<'a, 'c>,
            T::Vars,
        )
            -> Result<(RuleHeadBuilder<'a, 'c>, RuleBodyBuilder<'a, 'c>)>,
    ) -> Result<()>
    where
        T: RuleVars,
    {
        let Some(declaration) = self.relations.get(id) else {
                return error(Error::UnrecognizedRelation(id.to_string()));
            };

        let bound_vars = RefCell::new(HashMap::default());
        let head_builder = RuleHeadBuilder::new(declaration, &bound_vars);
        let body_builder = RuleBodyBuilder::new(&self.relations, &bound_vars);
        let vars = T::into_vars();

        let (h, b) = f(head_builder, body_builder, vars)?;

        let head = h.finalize()?;
        let body = b.finalize()?;

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

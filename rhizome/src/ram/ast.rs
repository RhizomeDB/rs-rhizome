use std::collections::BTreeMap;

use derive_more::{Constructor, Display, From, IsVariant, TryInto};

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
};

#[derive(Constructor, Clone, Debug, Display, From, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AliasId(usize);

impl AliasId {
    pub fn next(&self) -> Self {
        AliasId::new(self.0 + 1)
    }
}

#[derive(Clone, Debug)]
pub struct Program {
    statements: Vec<Statement>,
}

impl Program {
    pub fn new(statements: Vec<Statement>) -> Self {
        Self { statements }
    }

    pub fn statements(&self) -> &Vec<Statement> {
        &self.statements
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationBinding {
    id: RelationId,
    alias: Option<AliasId>,
}

impl RelationBinding {
    pub fn new(id: RelationId, alias: Option<AliasId>) -> Self {
        Self { id, alias }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn alias(&self) -> &Option<AliasId> {
        &self.alias
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Relation {
    id: RelationId,
    version: RelationVersion,
}

impl Relation {
    pub fn new(id: RelationId, version: RelationVersion) -> Self {
        Self { id, version }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn version(&self) -> &RelationVersion {
        &self.version
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationVersion {
    Total,
    Delta,
    New,
}

// TODO: Nested loops shouldn't be supported, so I should split the AST
// to make them unrepresentable.
#[derive(Clone, Debug, IsVariant)]
pub enum Statement {
    Insert { operation: Operation },
    Merge { from: Relation, into: Relation },
    Swap { left: Relation, right: Relation },
    Purge { relation: Relation },
    Loop { body: Vec<Statement> },
    Exit { relations: Vec<Relation> },
}

#[derive(Clone, Debug, IsVariant)]
pub enum Operation {
    Search {
        relation: Relation,
        alias: Option<AliasId>,
        when: Vec<Formula>,
        operation: Box<Operation>,
    },
    Project {
        attributes: BTreeMap<AttributeId, Term>,
        into: Relation,
    },
}

#[derive(Clone, Debug, IsVariant, From)]
pub enum Formula {
    Equality(Equality),
    NotIn(NotIn),
}

#[derive(Clone, Debug)]
pub struct Equality {
    left: Term,
    right: Term,
}

impl Equality {
    pub fn new(left: Term, right: Term) -> Self {
        Self { left, right }
    }

    pub fn left(&self) -> &Term {
        &self.left
    }

    pub fn right(&self) -> &Term {
        &self.right
    }
}

#[derive(Clone, Debug)]
pub struct NotIn {
    attributes: BTreeMap<AttributeId, Term>,
    relation: Relation,
}

impl NotIn {
    pub fn new(attributes: Vec<(AttributeId, Term)>, relation: Relation) -> Self {
        let attributes = BTreeMap::from_iter(attributes.into_iter());

        Self {
            attributes,
            relation,
        }
    }

    pub fn attributes(&self) -> &BTreeMap<AttributeId, Term> {
        &self.attributes
    }

    pub fn relation(&self) -> &Relation {
        &self.relation
    }
}

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Attribute(Attribute),
    Literal(Literal),
}

#[derive(Clone, Debug)]
pub struct Attribute {
    id: AttributeId,
    relation: RelationBinding,
}

impl Attribute {
    pub fn new(id: AttributeId, relation: RelationBinding) -> Self {
        Self { id, relation }
    }

    pub fn id(&self) -> &AttributeId {
        &self.id
    }

    pub fn relation(&self) -> &RelationBinding {
        &self.relation
    }
}

#[derive(Clone, Debug)]
pub struct Literal {
    datum: Datum,
}

impl Literal {
    pub fn new(datum: impl Into<Datum>) -> Self {
        Self {
            datum: datum.into(),
        }
    }

    pub fn datum(&self) -> &Datum {
        &self.datum
    }
}

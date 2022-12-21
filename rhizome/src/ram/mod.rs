use std::collections::BTreeMap;

use derive_more::{From, IsVariant, TryInto};

use crate::{
    datum::Datum,
    id::{AliasId, AttributeId, RelationId},
};

#[derive(Clone, Debug)]
pub struct Program {
    statements: Vec<Statement>,
}

impl Program {
    pub fn new(statements: Vec<Statement>) -> Self {
        Self {
            statements: statements,
        }
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

    pub fn id(&self) -> RelationId {
        self.id.clone()
    }

    pub fn version(&self) -> RelationVersion {
        self.version.clone()
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
}

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Attribute(Attribute),
    Literal(Literal),
}

#[derive(Clone, Debug)]
pub struct Attribute {
    id: AttributeId,
    relation: RelationId,
    alias: Option<AliasId>,
}

impl Attribute {
    pub fn new(id: AttributeId, relation: RelationId, alias: Option<AliasId>) -> Self {
        Self {
            id,
            relation,
            alias,
        }
    }

    pub fn id(&self) -> AttributeId {
        self.id.clone()
    }

    pub fn relation(&self) -> RelationId {
        self.relation.clone()
    }

    pub fn alias(&self) -> Option<AliasId> {
        self.alias.clone()
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

    pub fn datum(&self) -> Datum {
        self.datum.clone()
    }
}

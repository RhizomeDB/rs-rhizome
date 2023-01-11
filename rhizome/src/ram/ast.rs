use derive_more::{Display, From, IsVariant, TryInto};
use im::HashMap;

use crate::{
    datum::Datum,
    id::{AttributeId, RelationId},
};

#[derive(Clone, Copy, Debug, Display, From, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AliasId(usize);

impl AliasId {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for AliasId {
    fn default() -> Self {
        Self::new()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationRef {
    id: RelationId,
    version: RelationVersion,
}

impl RelationRef {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationVersion {
    Total,
    Delta,
    New,
}

// TODO: Nested loops shouldn't be supported, so I should split the AST
// to make them unrepresentable.
#[derive(Clone, Debug, IsVariant)]
pub enum Statement {
    Insert(Insert),
    Merge(Merge),
    Swap(Swap),
    Purge(Purge),
    Loop(Loop),
    Exit(Exit),
    Sinks(Sinks),
}

#[derive(Clone, Debug)]
pub struct Insert {
    operation: Operation,
}

impl Insert {
    pub fn new(operation: Operation) -> Self {
        Self { operation }
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Merge {
    from: RelationRef,
    into: RelationRef,
}

impl Merge {
    pub fn new(from: RelationRef, into: RelationRef) -> Self {
        Self { from, into }
    }

    pub fn from(&self) -> &RelationRef {
        &self.from
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Swap {
    left: RelationRef,
    right: RelationRef,
}

impl Swap {
    pub fn new(left: RelationRef, right: RelationRef) -> Self {
        Self { left, right }
    }

    pub fn left(&self) -> &RelationRef {
        &self.left
    }

    pub fn right(&self) -> &RelationRef {
        &self.right
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Purge {
    relation: RelationRef,
}

impl Purge {
    pub fn new(relation: RelationRef) -> Self {
        Self { relation }
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }
}

#[derive(Clone, Debug)]
pub struct Loop {
    body: Vec<Statement>,
}

impl Loop {
    pub fn new(body: Vec<Statement>) -> Self {
        Self { body }
    }

    pub fn body(&self) -> &Vec<Statement> {
        &self.body
    }
}

#[derive(Clone, Debug)]
pub struct Exit {
    relations: Vec<RelationRef>,
}

impl Exit {
    pub fn new(relations: Vec<RelationRef>) -> Self {
        Self { relations }
    }

    pub fn relations(&self) -> &Vec<RelationRef> {
        &self.relations
    }
}

#[derive(Clone, Debug)]
pub struct Sinks {
    relations: Vec<RelationRef>,
}

impl Sinks {
    pub fn new(relations: Vec<RelationRef>) -> Self {
        Self { relations }
    }

    pub fn relations(&self) -> &Vec<RelationRef> {
        &self.relations
    }
}

#[derive(Clone, Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
}

#[derive(Clone, Debug)]
pub struct Search {
    relation: RelationRef,
    alias: Option<AliasId>,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl Search {
    pub fn new(
        relation: RelationRef,
        alias: Option<AliasId>,
        when: Vec<Formula>,
        operation: Operation,
    ) -> Self {
        Self {
            relation,
            alias,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }

    pub fn alias(&self) -> &Option<AliasId> {
        &self.alias
    }

    pub fn when(&self) -> &Vec<Formula> {
        &self.when
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

#[derive(Clone, Debug)]
pub struct Project {
    attributes: HashMap<AttributeId, Term>,
    into: RelationRef,
}

impl Project {
    pub fn new(attributes: HashMap<AttributeId, Term>, into: RelationRef) -> Self {
        Self { attributes, into }
    }

    pub fn attributes(&self) -> &HashMap<AttributeId, Term> {
        &self.attributes
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

#[derive(Clone, Debug, IsVariant, From)]
pub enum Formula {
    Equality(Equality),
    NotIn(NotIn),
}

#[derive(Clone, Copy, Debug)]
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
    attributes: HashMap<AttributeId, Term>,
    relation: RelationRef,
}

impl NotIn {
    pub fn new(attributes: Vec<(AttributeId, Term)>, relation: RelationRef) -> Self {
        let attributes = HashMap::from_iter(attributes.into_iter());

        Self {
            attributes,
            relation,
        }
    }

    pub fn attributes(&self) -> &HashMap<AttributeId, Term> {
        &self.attributes
    }

    pub fn relation(&self) -> &RelationRef {
        &self.relation
    }
}

#[derive(Clone, Copy, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Attribute(Attribute),
    Literal(Literal),
}

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
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

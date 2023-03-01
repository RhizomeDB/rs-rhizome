use std::hash::Hash;

use derive_more::{Display, From, IsVariant, TryInto};
use im::{HashMap, HashSet};

use crate::{
    datum::Datum,
    id::{AttributeId, LinkId, RelationId, VariableId},
};

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    inputs: Vec<RelationId>,
    outputs: Vec<RelationId>,
    statements: Vec<Statement>,
}

impl Program {
    pub fn new(
        inputs: impl IntoIterator<Item = RelationId>,
        outputs: impl IntoIterator<Item = RelationId>,
        statements: impl IntoIterator<Item = Statement>,
    ) -> Self {
        let inputs = inputs.into_iter().collect();
        let outputs = outputs.into_iter().collect();
        let statements = statements.into_iter().collect();

        Self {
            inputs,
            outputs,
            statements,
        }
    }

    pub fn inputs(&self) -> &Vec<RelationId> {
        &self.inputs
    }

    pub fn outputs(&self) -> &Vec<RelationId> {
        &self.outputs
    }

    pub fn statements(&self) -> &Vec<Statement> {
        &self.statements
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationBinding {
    id: RelationId,
    source: RelationSource,
    alias: Option<AliasId>,
}

impl RelationBinding {
    pub fn new(id: impl Into<RelationId>, source: RelationSource, alias: Option<AliasId>) -> Self {
        let id = id.into();

        Self { id, source, alias }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn source(&self) -> RelationSource {
        self.source
    }

    pub fn alias(&self) -> &Option<AliasId> {
        &self.alias
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationRef {
    id: RelationId,
    source: RelationSource,
    version: RelationVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum RelationSource {
    EDB,
    IDB,
}

impl RelationRef {
    pub fn new(
        id: impl Into<RelationId>,
        source: RelationSource,
        version: RelationVersion,
    ) -> Self {
        let id = id.into();

        Self {
            id,
            source,
            version,
        }
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn source(&self) -> RelationSource {
        self.source
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
    Sources(Sources),
    Sinks(Sinks),
}

#[derive(Clone, Debug)]
pub struct Insert {
    operation: Operation,
    // Whether the insertion is for a ground atom with all constant attributes.
    // I don't love this, but it enables us to ensure ground facts are only inserted
    // into the delta relation once.
    is_ground: bool,
}

impl Insert {
    pub fn new(operation: Operation, is_ground: bool) -> Self {
        Self {
            operation,
            is_ground,
        }
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    pub fn is_ground(&self) -> bool {
        self.is_ground
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
    pub fn new(body: impl IntoIterator<Item = Statement>) -> Self {
        let body = body.into_iter().collect();

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
    pub fn new(relations: impl IntoIterator<Item = RelationRef>) -> Self {
        let relations = relations.into_iter().collect();

        Self { relations }
    }

    pub fn relations(&self) -> &Vec<RelationRef> {
        &self.relations
    }
}

#[derive(Clone, Debug)]
pub struct Sources {
    relations: HashSet<RelationRef>,
}

impl Sources {
    pub fn new(relations: impl IntoIterator<Item = RelationRef>) -> Self {
        let relations = relations.into_iter().collect();

        Self { relations }
    }

    pub fn relations(&self) -> &HashSet<RelationRef> {
        &self.relations
    }
}

#[derive(Clone, Debug)]
pub struct Sinks {
    relations: HashSet<RelationRef>,
}

impl Sinks {
    pub fn new(relations: impl IntoIterator<Item = RelationRef>) -> Self {
        let relations = relations.into_iter().collect();

        Self { relations }
    }

    pub fn relations(&self) -> &HashSet<RelationRef> {
        &self.relations
    }
}

#[derive(Clone, Debug, IsVariant)]
pub enum Operation {
    Search(Search),
    Project(Project),
    GetLink(GetLink),
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
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

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
    pub fn new<A, T>(attributes: impl IntoIterator<Item = (A, T)>, into: RelationRef) -> Self
    where
        A: Into<AttributeId>,
        T: Into<Term>,
    {
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        Self { attributes, into }
    }

    pub fn attributes(&self) -> &HashMap<AttributeId, Term> {
        &self.attributes
    }

    pub fn into(&self) -> &RelationRef {
        &self.into
    }
}

#[derive(Clone, Debug)]
pub struct GetLink {
    cid_term: Term,
    // TODO: It may be worthwhile to allow the LinkId to be unbound to support iterating through a CID's links,
    // but I'm not sure yet whether that has has implications on termination or convergence. It shouldn't,
    // since only EDB facts have links, and so the universe of LinkIds is finite, but we can keep things
    // simple to start.
    link_id: LinkId,
    link_value: Term,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl GetLink {
    pub fn new(
        cid_term: Term,
        link_id: LinkId,
        link_value: Term,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            cid_term,
            link_id,
            link_value,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn cid_term(&self) -> &Term {
        &self.cid_term
    }

    pub fn link_id(&self) -> &LinkId {
        &self.link_id
    }

    pub fn link_value(&self) -> &Term {
        &self.link_value
    }

    pub fn when(&self) -> &Vec<Formula> {
        &self.when
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

#[derive(Clone, Debug, IsVariant, From, TryInto)]
pub enum Formula {
    Equality(Equality),
    NotIn(NotIn),
}

impl Formula {
    pub fn equality(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        Self::Equality(Equality::new(left, right))
    }

    pub fn not_in<A, T>(attributes: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<AttributeId>,
        T: Into<Term>,
    {
        Self::NotIn(NotIn::new(attributes, relation))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Equality {
    left: Term,
    right: Term,
}

impl Equality {
    pub fn new(left: impl Into<Term>, right: impl Into<Term>) -> Self {
        let left = left.into();
        let right = right.into();

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
    pub fn new<A, T>(attributes: impl IntoIterator<Item = (A, T)>, relation: RelationRef) -> Self
    where
        A: Into<AttributeId>,
        T: Into<Term>,
    {
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

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
    Variable(Variable),
}

impl Term {
    pub fn attribute(id: impl Into<AttributeId>, relation: RelationBinding) -> Self {
        Self::Attribute(Attribute::new(id, relation))
    }

    pub fn literal(datum: impl Into<Datum>) -> Self {
        Self::Literal(Literal::new(datum))
    }

    pub fn variable(id: impl Into<VariableId>) -> Self {
        Self::Variable(Variable::new(id))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Attribute {
    id: AttributeId,
    relation: RelationBinding,
}

impl Attribute {
    pub fn new(id: impl Into<AttributeId>, relation: RelationBinding) -> Self {
        let id = id.into();

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

#[derive(Clone, Copy, Debug)]
pub struct Variable {
    id: VariableId,
}

impl Variable {
    pub fn new(id: impl Into<VariableId>) -> Self {
        let id = id.into();

        Self { id }
    }

    pub fn id(&self) -> &VariableId {
        &self.id
    }
}

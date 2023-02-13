use anyhow::Result;
use derive_more::{From, IsVariant, TryInto};
use im::{HashMap, HashSet};

use crate::{
    datum::Datum,
    error::{error, Error},
    id::{AttributeId, RelationId, VariableId},
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Program {
    statements: Vec<Statement>,
}

impl Program {
    pub fn new(statements: impl IntoIterator<Item = Statement>) -> Self {
        let statements = statements.into_iter().collect();

        Self { statements }
    }

    pub fn inputs(&self) -> Vec<InputSchema> {
        self.statements_of::<InputSchema>()
    }

    pub fn clauses(&self) -> Vec<Clause> {
        self.statements_of::<Clause>()
    }

    fn statements_of<T>(&self) -> Vec<T>
    where
        T: TryFrom<Statement>,
    {
        self.statements
            .iter()
            .filter_map(|statement| T::try_from(statement.clone()).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Stratum {
    relations: Vec<RelationId>,
    clauses: Vec<Clause>,
    is_recursive: bool,
}

impl Stratum {
    pub fn new(
        relations: impl IntoIterator<Item = impl Into<RelationId>>,
        clauses: impl IntoIterator<Item = Clause>,
        is_recursive: bool,
    ) -> Self {
        let relations = relations.into_iter().map(|v| v.into()).collect();
        let clauses = clauses.into_iter().collect();

        Self {
            relations,
            clauses,
            is_recursive,
        }
    }

    pub fn relations(&self) -> &Vec<RelationId> {
        &self.relations
    }

    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    pub fn facts(&self) -> Vec<Fact> {
        self.clauses_of::<Fact>()
    }

    pub fn rules(&self) -> Vec<Rule> {
        self.clauses_of::<Rule>()
    }

    fn clauses_of<T>(&self) -> Vec<T>
    where
        T: TryFrom<Clause>,
    {
        self.clauses
            .iter()
            .filter_map(|clause| T::try_from(clause.clone()).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum Statement {
    InputSchema(InputSchema),
    Clause(Clause),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InputSchema {
    id: RelationId,
    attributes: HashSet<AttributeId>,
}

impl InputSchema {
    pub fn new(
        id: impl Into<RelationId>,
        attributes: impl IntoIterator<Item = impl Into<AttributeId>>,
    ) -> Result<Self> {
        let mut uniq = HashSet::<AttributeId>::default();
        for id in attributes {
            let id = id.into();

            match uniq.insert(id) {
                Some(_) => return error(Error::DuplicateSchemaAttributeId(id)),
                None => continue,
            }
        }

        Ok(Self {
            id: id.into(),
            attributes: uniq,
        })
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn attributes(&self) -> &HashSet<AttributeId> {
        &self.attributes
    }
}

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum Clause {
    Fact(Fact),
    Rule(Rule),
}

impl Clause {
    pub fn fact(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, Literal)>,
    ) -> Self {
        Self::Fact(Fact::new(id, args))
    }

    pub fn rule(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
        body: impl IntoIterator<Item = BodyTerm>,
    ) -> Result<Self> {
        Ok(Self::Rule(Rule::new(id, args, body)?))
    }

    pub fn head(&self) -> &RelationId {
        match self {
            Clause::Fact(fact) => &fact.head,
            Clause::Rule(rule) => &rule.head,
        }
    }

    pub fn depends_on(&self) -> Vec<ClauseDependency> {
        match self {
            Self::Fact(fact) => fact.depends_on(),
            Self::Rule(rule) => rule.depends_on(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Fact {
    head: RelationId,
    args: HashMap<AttributeId, Literal>,
}

impl Fact {
    pub fn new(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, Literal)>,
    ) -> Self {
        Self {
            head: id.into(),
            args: args.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    pub fn head(&self) -> &RelationId {
        &self.head
    }

    pub fn args(&self) -> &HashMap<AttributeId, Literal> {
        &self.args
    }

    pub fn depends_on(&self) -> Vec<ClauseDependency> {
        Vec::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Rule {
    head: RelationId,
    args: HashMap<AttributeId, AttributeValue>,
    body: Vec<BodyTerm>,
}

impl Rule {
    pub fn new(
        relation_id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
        body: impl IntoIterator<Item = BodyTerm>,
    ) -> Result<Self> {
        let head = relation_id.into();
        let body: Vec<BodyTerm> = body.into_iter().collect();

        let args: HashMap<AttributeId, AttributeValue> =
            args.into_iter().map(|(k, v)| (k.into(), v)).collect();

        let mut positive_rhs_variables = HashSet::<Variable>::default();
        for term in &body {
            if let Some(BodyTermPolarity::Positive) = BodyTerm::polarity(term) {
                for variable in term.variables() {
                    positive_rhs_variables.insert(variable);
                }
            }
        }

        for (id, value) in args.clone() {
            if let AttributeValue::Variable(variable) = value {
                if !positive_rhs_variables.contains(&variable) {
                    return error(Error::RuleNotRangeRestricted(id, variable.id));
                }
            }
        }

        for term in &body {
            if let Ok(negation) = Negation::try_from(term.clone()) {
                for (attribute_id, value) in negation.args {
                    if let AttributeValue::Variable(variable) = value {
                        if !positive_rhs_variables.contains(&variable) {
                            return error(Error::RuleNotDomainIndependent(
                                negation.id,
                                attribute_id,
                                variable.id,
                            ));
                        }
                    }
                }
            }
        }

        Ok(Self { head, args, body })
    }

    pub fn head(&self) -> &RelationId {
        &self.head
    }

    pub fn args(&self) -> &HashMap<AttributeId, AttributeValue> {
        &self.args
    }

    pub fn body(&self) -> &Vec<BodyTerm> {
        &self.body
    }

    pub fn predicates(&self) -> Vec<Predicate> {
        self.body_terms_of::<Predicate>()
    }

    pub fn negations(&self) -> Vec<Negation> {
        self.body_terms_of::<Negation>()
    }

    pub fn depends_on(&self) -> Vec<ClauseDependency> {
        self.body
            .iter()
            .flat_map(|term| {
                if let Some(polarity) = term.polarity() {
                    term.depends_on()
                        .iter()
                        .map(|d| ClauseDependency {
                            from: *d,
                            to: self.head,
                            polarity,
                        })
                        .collect()
                } else {
                    Vec::default()
                }
            })
            .collect()
    }

    fn body_terms_of<T>(&self) -> Vec<T>
    where
        T: TryFrom<BodyTerm>,
    {
        self.body
            .iter()
            .filter_map(|term| T::try_from(term.clone()).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClauseDependency {
    from: RelationId,
    to: RelationId,
    polarity: BodyTermPolarity,
}

impl ClauseDependency {
    pub fn from(&self) -> &RelationId {
        &self.from
    }

    pub fn to(&self) -> &RelationId {
        &self.to
    }

    pub fn polarity(&self) -> &BodyTermPolarity {
        &self.polarity
    }
}

#[derive(Debug, Clone, Copy, IsVariant)]
pub enum BodyTermPolarity {
    Positive,
    Negative,
}

#[derive(Debug, Clone, Copy, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum AttributeValue {
    Literal(Literal),
    Variable(Variable),
}

impl AttributeValue {
    pub fn literal(datum: impl Into<Datum>) -> Self {
        Self::Literal(Literal::new(datum))
    }

    pub fn variable(id: impl Into<VariableId>) -> Self {
        Self::Variable(Variable::new(id))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub struct Variable {
    id: VariableId,
}

impl Variable {
    pub fn new(id: impl Into<VariableId>) -> Self {
        Self { id: id.into() }
    }
}

#[derive(Debug, Clone, Eq, From, PartialEq, IsVariant, TryInto)]
pub enum BodyTerm {
    Predicate(Predicate),
    Negation(Negation),
}

impl BodyTerm {
    pub fn predicate(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
    ) -> Self {
        Self::Predicate(Predicate::new(id, args))
    }

    pub fn negation(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
    ) -> Self {
        Self::Negation(Negation::new(id, args))
    }

    pub fn variables(&self) -> HashSet<Variable> {
        match self {
            BodyTerm::Predicate(predicate) => predicate.variables(),
            BodyTerm::Negation(negation) => negation.variables(),
        }
    }

    pub fn depends_on(&self) -> Vec<RelationId> {
        match self {
            BodyTerm::Predicate(predicate) => vec![predicate.id],
            BodyTerm::Negation(negation) => vec![negation.id],
        }
    }

    pub fn polarity(&self) -> Option<BodyTermPolarity> {
        match self {
            BodyTerm::Predicate(_) => Some(BodyTermPolarity::Positive),
            BodyTerm::Negation(_) => Some(BodyTermPolarity::Negative),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Predicate {
    id: RelationId,
    args: HashMap<AttributeId, AttributeValue>,
}

impl Predicate {
    pub fn new(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
    ) -> Self {
        Self {
            id: id.into(),
            args: args.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn args(&self) -> &HashMap<AttributeId, AttributeValue> {
        &self.args
    }

    pub fn variables(&self) -> HashSet<Variable> {
        self.args
            .iter()
            .filter_map(|(_, v)| Variable::try_from(*v).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Negation {
    id: RelationId,
    args: HashMap<AttributeId, AttributeValue>,
}

impl Negation {
    pub fn new(
        id: impl Into<RelationId>,
        args: impl IntoIterator<Item = (impl Into<AttributeId>, AttributeValue)>,
    ) -> Self {
        Self {
            id: id.into(),
            args: args.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    pub fn id(&self) -> &RelationId {
        &self.id
    }

    pub fn args(&self) -> &HashMap<AttributeId, AttributeValue> {
        &self.args
    }

    pub fn variables(&self) -> HashSet<Variable> {
        self.args
            .iter()
            .filter_map(|(_, v)| Variable::try_from(*v).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_range_restriction() {
        assert_eq!(
            Some(&Error::RuleNotRangeRestricted(
                AttributeId::new("p0"),
                VariableId::new("X")
            )),
            Rule::new(
                "p",
                [("p0", AttributeValue::variable("X"))],
                [BodyTerm::negation(
                    "q",
                    [("q0", AttributeValue::variable("X"))]
                )],
            )
            .unwrap_err()
            .downcast_ref(),
        );

        assert!(matches!(
            Rule::new(
                "p",
                [("p0", AttributeValue::variable("X"))],
                [
                    BodyTerm::predicate("t", [("t", AttributeValue::variable("X"))]),
                    BodyTerm::negation("q", [("q0", AttributeValue::variable("X"))]),
                ],
            ),
            Ok(_)
        ),);
    }

    #[test]
    fn test_domain_independence() {
        assert_eq!(
            Some(&Error::RuleNotDomainIndependent(
                RelationId::new("q"),
                AttributeId::new("q0"),
                VariableId::new("X")
            )),
            Rule::new(
                "p",
                [("p0", AttributeValue::variable("P"))],
                [
                    BodyTerm::predicate("t", [("t0", AttributeValue::variable("P"))]),
                    BodyTerm::negation("q", [("q0", AttributeValue::variable("X"))]),
                ],
            )
            .unwrap_err()
            .downcast_ref(),
        );

        assert!(matches!(
            Rule::new(
                "p",
                [("p0", AttributeValue::variable("X"))],
                [
                    BodyTerm::predicate("t", [("t0", AttributeValue::variable("X"))]),
                    BodyTerm::negation("q", [("q0", AttributeValue::variable("X"))]),
                ],
            ),
            Ok(_)
        ),);
    }
}

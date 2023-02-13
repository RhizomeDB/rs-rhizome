use std::collections::{BTreeSet, HashSet};

use anyhow::Result;
use im::{vector, HashMap, Vector};
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};

use crate::{
    error::{error, Error},
    id::{AttributeId, RelationId},
    ram::{
        self,
        ast::{Exit, Formula, Insert, Loop, Merge, Project, Purge, Search, Swap, Term},
    },
};

use super::ast::*;

pub fn lower_to_ram(program: &Program) -> Result<ram::ast::Program> {
    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    if !program.inputs().is_empty() {
        // Run sources for each input
        statements.push(ram::ast::Statement::Sources(ram::ast::Sources::new(
            program.inputs().iter().map(|schema| {
                ram::ast::RelationRef::new(*schema.id(), ram::ast::RelationVersion::Delta)
            }),
        )));

        // Merge facts from each source into Total
        for input in program.inputs() {
            statements.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(*input.id(), ram::ast::RelationVersion::Delta),
                ram::ast::RelationRef::new(*input.id(), ram::ast::RelationVersion::Total),
            )));
        }
    }

    for stratum in &stratify(program)? {
        let mut lowered = lower_stratum_to_ram(stratum)?;

        statements.append(&mut lowered);
    }

    // Purge all newly received input facts
    for input in program.inputs() {
        statements.push(ram::ast::Statement::Purge(Purge::new(
            ram::ast::RelationRef::new(*input.id(), ram::ast::RelationVersion::Delta),
        )));
    }

    Ok(ram::ast::Program::new(statements))
}

pub fn lower_stratum_to_ram(stratum: &Stratum) -> Result<Vec<ram::ast::Statement>> {
    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    if stratum.is_recursive() {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(&fact)?;

            statements.push(lowered);
        }

        // Partition the stratum's rules based on whether they depend on relations
        // that change during this stratum
        let (dynamic_rules, static_rules): (Vec<Rule>, Vec<Rule>) =
            stratum.rules().into_iter().partition(|r| {
                r.predicates()
                    .iter()
                    .any(|p| stratum.relations().contains(p.id()))
            });

        // Evaluate static rules out of the loop
        for rule in &static_rules {
            let mut lowered = lower_rule_to_ram(rule, stratum, ram::ast::RelationVersion::Delta)?;

            statements.append(&mut lowered);
        }

        // Merge the output of the static rules into total
        for relation in HashSet::<RelationId>::from_iter(static_rules.iter().map(|r| *r.head())) {
            statements.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(relation, ram::ast::RelationVersion::Delta),
                ram::ast::RelationRef::new(relation, ram::ast::RelationVersion::Total),
            )));
        }

        let mut loop_body: Vec<ram::ast::Statement> = Vec::default();

        // Purge new, computed during the last loop iteration
        for relation in stratum.relations() {
            loop_body.push(ram::ast::Statement::Purge(Purge::new(
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::New),
            )));
        }

        // Evaluate dynamic rules within the loop, inserting into new
        for rule in &dynamic_rules {
            let mut lowered = lower_rule_to_ram(rule, stratum, ram::ast::RelationVersion::New)?;

            loop_body.append(&mut lowered);
        }

        // Run sinks for the stratum
        loop_body.push(ram::ast::Statement::Sinks(ram::ast::Sinks::new(
            stratum.relations().iter().map(|relation| {
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Delta)
            }),
        )));

        // Exit the loop if all of the dynamic relations have reached a fixed point
        loop_body.push(ram::ast::Statement::Exit(Exit::new(
            stratum
                .relations()
                .iter()
                .map(|id| ram::ast::RelationRef::new(*id, ram::ast::RelationVersion::New)),
        )));

        // Merge new into total, then swap new and delta
        for relation in stratum.relations() {
            loop_body.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::New),
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Total),
            )));

            loop_body.push(ram::ast::Statement::Swap(Swap::new(
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::New),
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Delta),
            )));
        }

        statements.push(ram::ast::Statement::Loop(Loop::new(loop_body)));

        // Purge delta, computed during static rules
        for relation in stratum.relations() {
            statements.push(ram::ast::Statement::Purge(Purge::new(
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Delta),
            )));
        }
    } else {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(&fact)?;

            statements.push(lowered);
        }

        // Merge facts from Delta into Total
        for id in stratum
            .facts()
            .iter()
            .map(|f| f.head())
            .cloned()
            .collect::<BTreeSet<RelationId>>()
        {
            statements.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(id, ram::ast::RelationVersion::Delta),
                ram::ast::RelationRef::new(id, ram::ast::RelationVersion::Total),
            )));
        }

        // Evaluate all rules, inserting into Delta
        for rule in stratum.rules() {
            let mut lowered = lower_rule_to_ram(&rule, stratum, ram::ast::RelationVersion::Delta)?;

            statements.append(&mut lowered);
        }

        // Merge rules from Delta into Total
        for rule in stratum.rules() {
            statements.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(*rule.head(), ram::ast::RelationVersion::Delta),
                ram::ast::RelationRef::new(*rule.head(), ram::ast::RelationVersion::Total),
            )));
        }

        // Run sinks for the stratum
        statements.push(ram::ast::Statement::Sinks(ram::ast::Sinks::new(
            stratum.relations().iter().map(|relation| {
                ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Delta)
            }),
        )));
    };

    Ok(statements)
}

pub fn lower_fact_to_ram(fact: &Fact) -> Result<ram::ast::Statement> {
    let attributes = fact
        .args()
        .iter()
        .map(|(k, v)| (*k, ram::ast::Literal::new(*v.datum())));

    Ok(ram::ast::Statement::Insert(Insert::new(
        ram::ast::Operation::Project(Project::new(
            attributes,
            ram::ast::RelationRef::new(*fact.head(), ram::ast::RelationVersion::Delta),
        )),
        true,
    )))
}

struct TermMetadata {
    alias: Option<ram::ast::AliasId>,
    bindings: HashMap<Variable, ram::ast::Term>,
}

impl TermMetadata {
    fn new(alias: Option<ram::ast::AliasId>, bindings: HashMap<Variable, ram::ast::Term>) -> Self {
        Self { alias, bindings }
    }

    fn is_bound(&self, variable: &Variable) -> bool {
        self.bindings.contains_key(variable)
    }
}

pub fn lower_rule_to_ram(
    rule: &Rule,
    _stratum: &Stratum,
    version: ram::ast::RelationVersion,
) -> Result<Vec<ram::ast::Statement>> {
    let mut next_alias = HashMap::<RelationId, ram::ast::AliasId>::default();
    let mut bindings = HashMap::<Variable, ram::ast::Term>::default();
    let mut term_metadata = Vec::<(BodyTerm, TermMetadata)>::default();

    for body_term in rule.body() {
        match body_term {
            BodyTerm::Predicate(predicate) => {
                let alias = next_alias.get(predicate.id()).copied();

                next_alias = next_alias.update_with(
                    *predicate.id(),
                    ram::ast::AliasId::default(),
                    |old, _| old.next(),
                );

                for (attribute_id, attribute_value) in predicate.args().clone() {
                    match attribute_value {
                        AttributeValue::Literal(_) => continue,
                        AttributeValue::Variable(variable) if !bindings.contains_key(&variable) => {
                            bindings.insert(
                                variable,
                                Term::attribute(
                                    attribute_id,
                                    ram::ast::RelationBinding::new(*predicate.id(), alias),
                                ),
                            )
                        }
                        _ => continue,
                    };
                }

                term_metadata.push((
                    body_term.clone(),
                    TermMetadata::new(alias, bindings.clone()),
                ));
            }
            BodyTerm::Negation(_) => continue,
        }
    }

    let projection_attributes: HashMap<AttributeId, ram::ast::Term> = rule
        .args()
        .iter()
        .map(|(k, v)| match v {
            AttributeValue::Literal(c) => (*k, Term::literal(*c.datum())),
            AttributeValue::Variable(v) => (*k, *bindings.get(v).unwrap()),
        })
        .collect();

    let projection_variables: Vec<Variable> = rule
        .args()
        .iter()
        .filter_map(|(_, v)| match v {
            AttributeValue::Literal(_) => None,
            AttributeValue::Variable(variable) => Some(*variable),
        })
        .collect();

    let projection = ram::ast::Operation::Project(Project::new(
        projection_attributes.clone(),
        ram::ast::RelationRef::new(*rule.head(), version),
    ));

    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    // We use a bitmask to represent all of the possible rewrites of the rule under
    // semi-naive evaluation, i.e. those where at least one predicate searches
    // against a delta relation, rather than total.
    let rewrite_count = (1 << term_metadata.len()) - 1;

    for offset in 0..rewrite_count {
        // bitmask of dynamic predicate versions (1 => delta, 0 => total)
        let mask = (1 << term_metadata.len()) - 1 - offset;

        let mut negations = rule.negations().clone();
        let mut previous = projection.clone();
        for (i, (body_term, metadata)) in term_metadata.iter().rev().enumerate() {
            match body_term {
                BodyTerm::Predicate(predicate) => {
                    let mut formulae = Vec::default();
                    for (attribute_id, attribute_value) in predicate.args() {
                        match attribute_value {
                            AttributeValue::Literal(literal) => {
                                let formula = Formula::equality(
                                    ram::ast::Attribute::new(
                                        *attribute_id,
                                        ram::ast::RelationBinding::new(
                                            *predicate.id(),
                                            metadata.alias,
                                        ),
                                    ),
                                    ram::ast::Literal::new(*literal.datum()),
                                );

                                formulae.push(formula);
                            }
                            AttributeValue::Variable(variable) => {
                                match metadata.bindings.get(variable) {
                                    None => (),
                                    Some(ram::ast::Term::Attribute(inner))
                                        if *inner.relation().id() == *predicate.id()
                                            && *inner.relation().alias() == metadata.alias => {}
                                    Some(bound_value) => {
                                        let formula = Formula::equality(
                                            ram::ast::Attribute::new(
                                                *attribute_id,
                                                ram::ast::RelationBinding::new(
                                                    *predicate.id(),
                                                    metadata.alias,
                                                ),
                                            ),
                                            *bound_value,
                                        );

                                        formulae.push(formula);
                                    }
                                }
                            }
                        }
                    }

                    if predicate.id() == rule.head()
                        && projection_variables.iter().all(|v| metadata.is_bound(v))
                    {
                        formulae.push(Formula::not_in(
                            Vec::from_iter(projection_attributes.clone()),
                            ram::ast::RelationRef::new(
                                *rule.head(),
                                ram::ast::RelationVersion::Total,
                            ),
                        ))
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) =
                        negations.into_iter().partition(|n| {
                            n.variables()
                                .iter()
                                .all(|v| metadata.bindings.contains_key(v))
                        });

                    negations = unsatisfied;

                    for negation in satisfied {
                        let attributes = negation.args().iter().map(|(k, v)| match v {
                            AttributeValue::Literal(literal) => {
                                (*k, Term::literal(*literal.datum()))
                            }
                            AttributeValue::Variable(variable) => {
                                (*k, *metadata.bindings.get(variable).unwrap())
                            }
                        });

                        formulae.push(Formula::not_in(
                            attributes,
                            ram::ast::RelationRef::new(
                                *negation.id(),
                                ram::ast::RelationVersion::Total,
                            ),
                        ))
                    }

                    let version = if mask & (1 << i) != 0 {
                        ram::ast::RelationVersion::Delta
                    } else {
                        ram::ast::RelationVersion::Total
                    };

                    previous = ram::ast::Operation::Search(Search::new(
                        ram::ast::RelationRef::new(*predicate.id(), version),
                        metadata.alias,
                        formulae,
                        previous,
                    ));
                }
                BodyTerm::Negation(_) => unreachable!("Only iterating through positive terms"),
            };
        }

        statements.push(ram::ast::Statement::Insert(Insert::new(previous, false)));
    }

    Ok(statements)
}

pub fn stratify(program: &Program) -> Result<Vec<Stratum>> {
    let mut clauses_by_relation = HashMap::<RelationId, Vector<Clause>>::default();

    for clause in program.clauses() {
        clauses_by_relation = clauses_by_relation.alter(
            |old| match old {
                Some(clauses) => {
                    let mut new = clauses;
                    new.push_back(clause.clone());

                    Some(new)
                }
                None => Some(vector![clause.clone()]),
            },
            *clause.head(),
        );
    }

    let mut edg = DiGraph::<RelationId, BodyTermPolarity>::default();
    let mut nodes = HashMap::<RelationId, NodeIndex>::default();

    for clause in program.clauses() {
        nodes = nodes.alter(
            |old| match old {
                Some(id) => Some(id),
                None => Some(edg.add_node(*clause.head())),
            },
            *clause.head(),
        );

        for dependency in clause.depends_on() {
            nodes = nodes.alter(
                |old| match old {
                    Some(id) => Some(id),
                    None => Some(edg.add_node(*dependency.to())),
                },
                *dependency.to(),
            );

            nodes = nodes.alter(
                |old| match old {
                    Some(id) => Some(id),
                    None => Some(edg.add_node(*dependency.from())),
                },
                *dependency.from(),
            );

            let to = nodes.get(dependency.to()).unwrap();
            let from = nodes.get(dependency.from()).unwrap();

            edg.add_edge(*from, *to, *dependency.polarity());
        }
    }

    let sccs = petgraph::algo::kosaraju_scc(&edg);

    for scc in &sccs {
        for node in scc {
            for edge in edg.edges_directed(*node, Direction::Outgoing) {
                if edge.weight().is_negative() && scc.contains(&edge.target()) {
                    return error(Error::ProgramUnstratifiable);
                }
            }
        }
    }

    Ok(sccs
        .iter()
        .map(|nodes| {
            Stratum::new(
                nodes.iter().map(|n| edg.node_weight(*n).unwrap()).cloned(),
                nodes.iter().flat_map(|n| {
                    let weight = edg.node_weight(*n).unwrap();

                    clauses_by_relation.get(weight).cloned().unwrap_or_default()
                }),
                nodes.len() > 1 || edg.contains_edge(nodes[0], nodes[0]),
            )
        })
        .rev()
        .collect())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::logic::parser;

    use super::*;

    #[test]
    fn stratify_tests() -> Result<()> {
        let program = parser::parse(
            r#"
        v(v: X) :- r(r0: X, r1: Y).
        v(v: Y) :- r(r0: X, r1: Y).

        t(t0: X, t1: Y) :- r(r0: X, r1: Y).
        t(t0: X, t1: Y) :- t(t0: X, t1: Z), r(r0: Z, r1: Y).

        tc(tc0: X, tc1: Y):- v(v: X), v(v: Y), !t(t0: X, t1: Y).
        "#,
        )?;

        assert_eq!(
            vec![
                Stratum::new(["r"], [], false,),
                Stratum::new(
                    ["v"],
                    [
                        Clause::rule(
                            "v",
                            [("v", AttributeValue::variable("X"))],
                            [BodyTerm::predicate(
                                "r",
                                [
                                    ("r0", AttributeValue::variable("X")),
                                    ("r1", AttributeValue::variable("Y")),
                                ],
                            )],
                        )?,
                        Clause::rule(
                            "v",
                            [("v", AttributeValue::variable("Y"))],
                            [BodyTerm::predicate(
                                "r",
                                [
                                    ("r0", AttributeValue::variable("X")),
                                    ("r1", AttributeValue::variable("Y")),
                                ],
                            )],
                        )?,
                    ],
                    false,
                ),
                Stratum::new(
                    ["t"],
                    [
                        Clause::rule(
                            "t",
                            [
                                ("t0", AttributeValue::variable("X")),
                                ("t1", AttributeValue::variable("Y")),
                            ],
                            [BodyTerm::predicate(
                                "r",
                                [
                                    ("r0", AttributeValue::variable("X")),
                                    ("r1", AttributeValue::variable("Y")),
                                ],
                            )],
                        )?,
                        Clause::rule(
                            "t",
                            [
                                ("t0", AttributeValue::variable("X")),
                                ("t1", AttributeValue::variable("Y")),
                            ],
                            [
                                BodyTerm::predicate(
                                    "t",
                                    [
                                        ("t0", AttributeValue::variable("X")),
                                        ("t1", AttributeValue::variable("Z")),
                                    ],
                                ),
                                BodyTerm::predicate(
                                    "r",
                                    [
                                        ("r0", AttributeValue::variable("Z")),
                                        ("r1", AttributeValue::variable("Y")),
                                    ],
                                ),
                            ],
                        )?,
                    ],
                    true,
                ),
                Stratum::new(
                    ["tc"],
                    [Clause::rule(
                        "tc",
                        [
                            ("tc0", AttributeValue::variable("X")),
                            ("tc1", AttributeValue::variable("Y")),
                        ],
                        [
                            BodyTerm::predicate("v", [("v", AttributeValue::variable("X"))],),
                            BodyTerm::predicate("v", [("v", AttributeValue::variable("Y"))],),
                            BodyTerm::negation(
                                "t",
                                [
                                    ("t0", AttributeValue::variable("X")),
                                    ("t1", AttributeValue::variable("Y")),
                                ],
                            ),
                        ],
                    )?],
                    false,
                )
            ],
            stratify(&program)?
        );

        Ok(())
    }

    #[test]
    fn unstratifiable_tests() -> Result<()> {
        let program = parser::parse(
            r#"
        p(p: X) :- t(t: X), !q(q: X).
        q(q: X) :- t(t: X), !p(p: X)."#,
        )?;

        assert_eq!(
            Some(&Error::ProgramUnstratifiable),
            stratify(&program).unwrap_err().downcast_ref()
        );

        Ok(())
    }
}

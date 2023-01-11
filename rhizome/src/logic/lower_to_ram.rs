use anyhow::Result;
use im::{vector, HashMap, HashSet, Vector};
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};

use crate::{
    error::{error, Error},
    id::RelationId,
    ram::{
        self,
        ast::{Exit, Insert, Loop, Merge, Project, Purge, Search, Swap},
    },
};

use super::ast::*;

pub fn lower_to_ram(program: &Program) -> Result<ram::ast::Program> {
    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    // Run sources for each input
    statements.push(ram::ast::Statement::Sources(ram::ast::Sources::new(
        program
            .inputs()
            .iter()
            .map(|schema| {
                ram::ast::RelationRef::new(*schema.id(), ram::ast::RelationVersion::Total)
            })
            .collect(),
    )));

    for stratum in &stratify(program)? {
        let mut lowered = lower_stratum_to_ram(stratum)?;

        statements.append(&mut lowered);
    }

    Ok(ram::ast::Program::new(statements))
}

pub fn lower_stratum_to_ram(stratum: &Stratum) -> Result<Vec<ram::ast::Statement>> {
    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    if stratum.is_recursive() {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(&fact, ram::ast::RelationVersion::Delta)?;

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
            let mut lowered = lower_rule_to_ram(rule, stratum, ram::ast::RelationVersion::Total)?;

            statements.append(&mut lowered);
        }

        // Merge the output of the static rules into delta, to be used in the loop
        for relation in HashSet::<RelationId>::from_iter(static_rules.iter().map(|r| *r.head())) {
            statements.push(ram::ast::Statement::Merge(Merge::new(
                ram::ast::RelationRef::new(relation, ram::ast::RelationVersion::Total),
                ram::ast::RelationRef::new(relation, ram::ast::RelationVersion::Delta),
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

        // Exit the loop if all of the dynamic relations have reached a fixed point
        loop_body.push(ram::ast::Statement::Exit(Exit::new(
            stratum
                .relations()
                .iter()
                .map(|id| ram::ast::RelationRef::new(*id, ram::ast::RelationVersion::New))
                .collect(),
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

        statements.push(ram::ast::Statement::Loop(Loop::new(loop_body)))
    } else {
        // Merge facts into total
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(&fact, ram::ast::RelationVersion::Total)?;

            statements.push(lowered);
        }

        // Evaluate all rules, inserting into total
        for rule in stratum.rules() {
            let mut lowered = lower_rule_to_ram(&rule, stratum, ram::ast::RelationVersion::Total)?;

            statements.append(&mut lowered);
        }
    };

    // Run sinks for the stratum
    // TODO: This always runs the sinks for the total relation, but we'll want
    // to better support running them for both total and delta
    statements.push(ram::ast::Statement::Sinks(ram::ast::Sinks::new(
        stratum
            .relations()
            .iter()
            .map(|relation| ram::ast::RelationRef::new(*relation, ram::ast::RelationVersion::Total))
            .collect(),
    )));

    Ok(statements)
}

pub fn lower_fact_to_ram(
    fact: &Fact,
    version: ram::ast::RelationVersion,
) -> Result<ram::ast::Statement> {
    let attributes = fact
        .args()
        .iter()
        .map(|(k, v)| (*k, ram::ast::Literal::new(*v.datum()).into()))
        .collect();

    Ok(ram::ast::Statement::Insert(Insert::new(
        ram::ast::Operation::Project(Project::new(
            attributes,
            ram::ast::RelationRef::new(*fact.head(), version),
        )),
    )))
}

struct TermMetadata {
    alias: Option<ram::ast::AliasId>,
    bindings: HashMap<Variable, ram::ast::Term>,
    is_dynamic: bool,
}

impl TermMetadata {
    fn new(
        alias: Option<ram::ast::AliasId>,
        bindings: HashMap<Variable, ram::ast::Term>,
        is_dynamic: bool,
    ) -> Self {
        Self {
            alias,
            bindings,
            is_dynamic,
        }
    }
}

pub fn lower_rule_to_ram(
    rule: &Rule,
    stratum: &Stratum,
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
                                ram::ast::Attribute::new(
                                    attribute_id,
                                    ram::ast::RelationBinding::new(*predicate.id(), alias),
                                )
                                .into(),
                            )
                        }
                        _ => continue,
                    };
                }

                term_metadata.push((
                    body_term.clone(),
                    TermMetadata::new(
                        alias,
                        bindings.clone(),
                        stratum.is_recursive() && stratum.relations().contains(predicate.id()),
                    ),
                ));
            }
            BodyTerm::Negation(_) => continue,
        }
    }

    let projection_attributes = rule
        .args()
        .iter()
        .map(|(k, v)| match v {
            AttributeValue::Literal(c) => (*k, ram::ast::Literal::new(*c.datum()).into()),
            AttributeValue::Variable(v) => (*k, *bindings.get(v).unwrap()),
        })
        .collect();

    let projection = ram::ast::Operation::Project(Project::new(
        projection_attributes,
        ram::ast::RelationRef::new(*rule.head(), version),
    ));

    let mut statements: Vec<ram::ast::Statement> = Vec::default();

    // We use a bitmask to represent all of the possible rewrites of the rule under
    // semi-naive evaluation, i.e. those where at least one dynamic predicate searches
    // against a delta relation, rather than total.
    //
    // TODO: Use Arc to share suffixes of a ram operation across overlapping insertions.
    // TODO: Decompose the rule into binary joins to reuse intermediate results.
    let count_of_dynamic = term_metadata
        .iter()
        .filter(|(_, metadata)| metadata.is_dynamic)
        .count();

    let rewrite_count = if count_of_dynamic == 0 {
        1
    } else {
        (1 << count_of_dynamic) - 1
    };

    for offset in 0..rewrite_count {
        // bitmask of dynamic predicate versions (1 => delta, 0 => total)
        let mask = (1 << count_of_dynamic) - 1 - offset;
        // Number of dynamic predicates handled so far
        let mut i = 0;

        let mut negations = rule.negations().clone();
        let mut previous = projection.clone();
        for (body_term, metadata) in term_metadata.iter().rev() {
            match body_term {
                BodyTerm::Predicate(predicate) => {
                    let mut formulae: Vec<ram::ast::Formula> = Vec::default();
                    for (attribute_id, attribute_value) in predicate.args() {
                        match attribute_value {
                            AttributeValue::Literal(literal) => {
                                let formula = ram::ast::Equality::new(
                                    ram::ast::Attribute::new(
                                        *attribute_id,
                                        ram::ast::RelationBinding::new(
                                            *predicate.id(),
                                            metadata.alias,
                                        ),
                                    )
                                    .into(),
                                    ram::ast::Literal::new(*literal.datum()).into(),
                                )
                                .into();

                                formulae.push(formula);
                            }
                            AttributeValue::Variable(variable) => {
                                match metadata.bindings.get(variable) {
                                    None => (),
                                    Some(ram::ast::Term::Attribute(inner))
                                        if *inner.relation().id() == *predicate.id()
                                            && *inner.relation().alias() == metadata.alias => {}
                                    Some(bound_value) => {
                                        let formula = ram::ast::Equality::new(
                                            ram::ast::Attribute::new(
                                                *attribute_id,
                                                ram::ast::RelationBinding::new(
                                                    *predicate.id(),
                                                    metadata.alias,
                                                ),
                                            )
                                            .into(),
                                            *bound_value,
                                        )
                                        .into();

                                        formulae.push(formula);
                                    }
                                }
                            }
                        }
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) =
                        negations.into_iter().partition(|n| {
                            n.variables()
                                .iter()
                                .all(|v| metadata.bindings.contains_key(v))
                        });

                    negations = unsatisfied;

                    for negation in satisfied {
                        let attributes = negation
                            .args()
                            .iter()
                            .map(|(k, v)| match v {
                                AttributeValue::Literal(literal) => {
                                    (*k, ram::ast::Literal::new(*literal.datum()).into())
                                }
                                AttributeValue::Variable(variable) => {
                                    (*k, *metadata.bindings.get(variable).unwrap())
                                }
                            })
                            .collect();

                        formulae.push(
                            ram::ast::NotIn::new(
                                attributes,
                                ram::ast::RelationRef::new(
                                    *negation.id(),
                                    ram::ast::RelationVersion::Total,
                                ),
                            )
                            .into(),
                        )
                    }

                    let version = if metadata.is_dynamic && (mask & (1 << i) != 0) {
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

            if metadata.is_dynamic {
                i += 1;
            }
        }

        statements.push(ram::ast::Statement::Insert(Insert::new(previous)));
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
                nodes
                    .iter()
                    .map(|n| edg.node_weight(*n).unwrap())
                    .cloned()
                    .collect(),
                nodes
                    .iter()
                    .flat_map(|n| {
                        let weight = edg.node_weight(*n).unwrap();

                        clauses_by_relation.get(weight).cloned().unwrap_or_default()
                    })
                    .collect(),
                // TODO: is this sufficient?
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
    fn stratify_tests() {
        let program = parser::parse(
            r#"
        v(v: X) :- r(r0: X, r1: Y).
        v(v: Y) :- r(r0: X, r1: Y).

        t(t0: X, t1: Y) :- r(r0: X, r1: Y).
        t(t0: X, t1: Y) :- t(t0: X, t1: Z), r(r0: Z, r1: Y).

        tc(tc0: X, tc1: Y):- v(v: X), v(v: Y), !t(t0: X, t1: Y).
        "#,
        )
        .unwrap();

        assert_eq!(
            vec![
                Stratum::new(vec![RelationId::new("r")], vec![], false,),
                Stratum::new(
                    vec![RelationId::new("v")],
                    vec![
                        Rule::new(
                            "v",
                            vec![("v", Variable::new("X").into())],
                            vec![Predicate::new(
                                "r",
                                vec![
                                    ("r0", Variable::new("X").into()),
                                    ("r1", Variable::new("Y").into()),
                                ],
                            )
                            .into()],
                        )
                        .unwrap()
                        .into(),
                        Rule::new(
                            "v",
                            vec![("v", Variable::new("Y").into())],
                            vec![Predicate::new(
                                "r",
                                vec![
                                    ("r0", Variable::new("X").into()),
                                    ("r1", Variable::new("Y").into()),
                                ],
                            )
                            .into()],
                        )
                        .unwrap()
                        .into(),
                    ],
                    false,
                ),
                Stratum::new(
                    vec![RelationId::new("t")],
                    vec![
                        Rule::new(
                            "t",
                            vec![
                                ("t0", Variable::new("X").into()),
                                ("t1", Variable::new("Y").into()),
                            ],
                            vec![Predicate::new(
                                "r",
                                vec![
                                    ("r0", Variable::new("X").into()),
                                    ("r1", Variable::new("Y").into()),
                                ],
                            )
                            .into()],
                        )
                        .unwrap()
                        .into(),
                        Rule::new(
                            "t",
                            vec![
                                ("t0", Variable::new("X").into()),
                                ("t1", Variable::new("Y").into()),
                            ],
                            vec![
                                Predicate::new(
                                    "t",
                                    vec![
                                        ("t0", Variable::new("X").into()),
                                        ("t1", Variable::new("Z").into()),
                                    ],
                                )
                                .into(),
                                Predicate::new(
                                    "r",
                                    vec![
                                        ("r0", Variable::new("Z").into()),
                                        ("r1", Variable::new("Y").into()),
                                    ],
                                )
                                .into(),
                            ],
                        )
                        .unwrap()
                        .into(),
                    ],
                    true,
                ),
                Stratum::new(
                    vec![RelationId::new("tc")],
                    vec![Rule::new(
                        "tc",
                        vec![
                            ("tc0", Variable::new("X").into()),
                            ("tc1", Variable::new("Y").into()),
                        ],
                        vec![
                            Predicate::new("v", vec![("v", Variable::new("X").into())],).into(),
                            Predicate::new("v", vec![("v", Variable::new("Y").into())],).into(),
                            Negation::new(
                                "t",
                                vec![
                                    ("t0", Variable::new("X").into()),
                                    ("t1", Variable::new("Y").into()),
                                ],
                            )
                            .into(),
                        ],
                    )
                    .unwrap()
                    .into(),],
                    false,
                )
            ],
            stratify(&program).unwrap()
        );
    }

    #[test]
    fn unstratifiable_tests() {
        let program = parser::parse(
            r#"
        p(p: X) :- t(t: X), !q(q: X).
        q(q: X) :- t(t: X), !p(p: X)."#,
        )
        .unwrap();

        assert_eq!(
            Some(&Error::ProgramUnstratifiable),
            stratify(&program).unwrap_err().downcast_ref()
        );
    }
}

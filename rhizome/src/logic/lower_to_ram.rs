use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};

use crate::{
    error::{error, Error},
    id::{ColumnId, RelationId, VarId},
    ram::{
        self,
        alias_id::AliasId,
        formula::Formula,
        operation::{get_link::GetLink, project::Project, search::Search, Operation},
        relation_binding::RelationBinding,
        relation_ref::RelationRef,
        relation_version::RelationVersion,
        statement::{
            exit::Exit, insert::Insert, merge::Merge, purge::Purge, recursive::Loop, sinks::Sinks,
            sources::Sources, swap::Swap, Statement,
        },
        term::Term,
    },
    relation::{EDB, IDB},
    value::Value,
};

use super::ast::{
    body_term::BodyTerm,
    cid_value::CidValue,
    clause::Clause,
    column_value::ColumnValue,
    declaration::{Declaration, InnerDeclaration},
    dependency::{Node, Polarity},
    fact::Fact,
    program::Program,
    rule::Rule,
    stratum::Stratum,
};

pub fn lower_to_ram(program: &Program) -> Result<ram::program::Program> {
    let mut inputs: Vec<Arc<InnerDeclaration<EDB>>> = Vec::default();
    let mut outputs: Vec<Arc<InnerDeclaration<IDB>>> = Vec::default();
    let mut statements: Vec<Statement> = Vec::default();

    for declaration in program.declarations() {
        match &**declaration {
            Declaration::EDB(inner) => {
                inputs.push(Arc::clone(inner));
            }
            Declaration::IDB(inner) => {
                outputs.push(Arc::clone(inner));
            }
        }
    }

    if !inputs.is_empty() {
        let relations: HashSet<_> = inputs.iter().map(|r| r.id()).collect();

        // Run sources for each input
        statements.push(Statement::Sources(Sources::from_iter(relations)));
    }

    for stratum in &stratify(program)? {
        let mut lowered = lower_stratum_to_ram(stratum, program)?;

        statements.append(&mut lowered);
    }

    // Purge all newly received input facts
    for relation in &inputs {
        let relation_ref = RelationRef::edb(relation.id(), RelationVersion::Delta);

        statements.push(Statement::Purge(Purge::new(relation_ref)));
    }

    Ok(ram::program::Program::new(inputs, outputs, statements))
}

pub fn lower_stratum_to_ram(stratum: &Stratum, program: &Program) -> Result<Vec<Statement>> {
    let mut statements: Vec<Statement> = Vec::default();

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
                r.predicates().iter().any(|p| match &**p.relation() {
                    Declaration::EDB(_) => false,
                    Declaration::IDB(inner) => stratum.relations().contains(&inner.id()),
                })
            });

        // Evaluate static rules out of the loop
        for rule in &static_rules {
            let mut lowered = lower_rule_to_ram(rule, stratum, program, RelationVersion::Delta)?;

            statements.append(&mut lowered);
        }

        // Merge the output of the static rules into total
        for relation in HashSet::<RelationId>::from_iter(static_rules.iter().map(|r| r.head())) {
            statements.push(Statement::Merge(Merge::new(
                RelationRef::idb(relation, RelationVersion::Delta),
                RelationRef::idb(relation, RelationVersion::Total),
            )));
        }

        let mut loop_body: Vec<Statement> = Vec::default();

        // Purge new, computed during the last loop iteration
        for relation in stratum.relations() {
            loop_body.push(Statement::Purge(Purge::new(RelationRef::idb(
                *relation,
                RelationVersion::New,
            ))));
        }

        // Evaluate dynamic rules within the loop, inserting into new
        for rule in &dynamic_rules {
            let mut lowered = lower_rule_to_ram(rule, stratum, program, RelationVersion::New)?;

            loop_body.append(&mut lowered);
        }

        // Run sinks for the stratum
        loop_body.push(Statement::Sinks(Sinks::new(
            stratum
                .relations()
                .iter()
                .map(|&relation| RelationRef::idb(relation, RelationVersion::Delta)),
        )));

        // Exit the loop if all of the dynamic relations have reached a fixed point
        loop_body.push(Statement::Exit(Exit::new(
            stratum
                .relations()
                .iter()
                .map(|&id| RelationRef::idb(id, RelationVersion::New)),
        )));

        // Merge new into total, then swap new and delta
        for &relation in stratum.relations() {
            loop_body.push(Statement::Merge(Merge::new(
                RelationRef::idb(relation, RelationVersion::New),
                RelationRef::idb(relation, RelationVersion::Total),
            )));

            loop_body.push(Statement::Swap(Swap::new(
                RelationRef::idb(relation, RelationVersion::New),
                RelationRef::idb(relation, RelationVersion::Delta),
            )));
        }

        statements.push(Statement::Loop(Loop::new(loop_body)));

        // Merge total into delta for subsequent strata
        // TODO: this seems wrong and will lead to duplicate work across epochs. Will likely need to
        // use the lattice based timestamps to resolve that.
        for &relation in stratum.relations() {
            statements.push(Statement::Merge(Merge::new(
                RelationRef::idb(relation, RelationVersion::Total),
                RelationRef::idb(relation, RelationVersion::Delta),
            )));

            // statements.push(Statement::Purge(Purge::new(RelationRef::new(
            //     *relation,
            //     RelationSource::IDB,
            //     RelationVersion::Delta,
            // ))));
        }
    } else {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(&fact)?;

            statements.push(lowered);
        }

        // Evaluate all rules, inserting into Delta
        for rule in stratum.rules() {
            let mut lowered = lower_rule_to_ram(&rule, stratum, program, RelationVersion::Delta)?;

            statements.append(&mut lowered);
        }

        // Merge rules from Delta into Total
        for relation in stratum.relations() {
            statements.push(Statement::Merge(Merge::new(
                RelationRef::idb(*relation, RelationVersion::Delta),
                RelationRef::idb(*relation, RelationVersion::Total),
            )));
        }

        // Run sinks for the stratum
        statements.push(Statement::Sinks(Sinks::new(
            stratum
                .relations()
                .iter()
                .map(|relation| RelationRef::idb(*relation, RelationVersion::Delta)),
        )));
    };

    Ok(statements)
}

pub fn lower_fact_to_ram(fact: &Fact) -> Result<Statement> {
    let attributes = fact
        .args()
        .iter()
        .map(|(k, v)| (*k, Term::Literal(v.clone())));

    Ok(Statement::Insert(Insert::new(
        Operation::Project(Project::new(
            attributes,
            RelationRef::idb(fact.head(), RelationVersion::Delta),
        )),
        true,
    )))
}

struct TermMetadata {
    alias: Option<AliasId>,
    bindings: im::HashMap<VarId, Term>,
}

impl TermMetadata {
    fn new(alias: Option<AliasId>, bindings: im::HashMap<VarId, Term>) -> Self {
        Self { alias, bindings }
    }

    fn is_bound(&self, variable: VarId) -> bool {
        self.bindings.contains_key(&variable)
    }
}

pub fn lower_rule_to_ram(
    rule: &Rule,
    _stratum: &Stratum,
    _program: &Program,
    version: RelationVersion,
) -> Result<Vec<Statement>> {
    let mut next_alias = im::HashMap::<RelationId, AliasId>::default();
    let mut bindings = im::HashMap::<VarId, Term>::default();
    let mut term_metadata = Vec::<(BodyTerm, TermMetadata)>::default();

    for body_term in rule.body() {
        match body_term {
            BodyTerm::Predicate(predicate) => {
                let alias = next_alias.get(&predicate.relation().id()).copied();

                next_alias = next_alias.update_with(
                    predicate.relation().id(),
                    AliasId::default(),
                    |old, _| old.next(),
                );

                for (attribute_id, attribute_value) in predicate.args().clone() {
                    match attribute_value {
                        ColumnValue::Literal(_) => continue,
                        ColumnValue::Binding(var) if !bindings.contains_key(&var.id()) => {
                            let binding = match &**predicate.relation() {
                                Declaration::EDB(inner) => RelationBinding::edb(inner.id(), alias),
                                Declaration::IDB(inner) => RelationBinding::idb(inner.id(), alias),
                            };

                            bindings.insert(var.id(), Term::Attribute(attribute_id, binding))
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
            BodyTerm::GetLink(inner) => {
                for v in inner.variables() {
                    if !bindings.contains_key(&v) {
                        bindings.insert(v, Term::Variable(v));
                    }
                }

                term_metadata.push((body_term.clone(), TermMetadata::new(None, bindings.clone())));
            }
        }
    }

    let projection_attributes: im::HashMap<ColumnId, Term> = rule
        .args()
        .iter()
        .map(|(k, v)| match v {
            ColumnValue::Literal(c) => (*k, Term::Literal(c.clone())),
            ColumnValue::Binding(v) => (*k, bindings.get(&v.id()).unwrap().clone()),
        })
        .collect();

    let projection_variables: Vec<VarId> = rule
        .args()
        .iter()
        .filter_map(|(_, v)| match *v {
            ColumnValue::Literal(_) => None,
            ColumnValue::Binding(var) => Some(var.id()),
        })
        .collect();

    let projection = Operation::Project(Project::new(
        projection_attributes.clone(),
        RelationRef::idb(rule.head(), version),
    ));

    let mut statements: Vec<Statement> = Vec::default();

    // We use a bitmask to represent all of the possible rewrites of the rule under
    // semi-naive evaluation, i.e. those where at least one predicate searches
    // against a delta relation, rather than total.
    let rewrite_count = (1 << term_metadata.len()) - 1;

    for offset in 0..rewrite_count {
        // bitmask of dynamic predicate versions (1 => delta, 0 => total)
        let mask = (1 << term_metadata.len()) - 1 - offset;

        let mut negations = rule.negations().clone();
        let mut previous = projection.clone();

        // TODO: Hack to handle skipping rewrites involving static terms
        let mut skip = false;

        for (i, (body_term, metadata)) in term_metadata.iter().rev().enumerate() {
            match body_term {
                BodyTerm::Predicate(predicate) => {
                    let mut formulae = Vec::default();

                    for (&attribute_id, attribute_value) in predicate.args() {
                        let binding = match &**predicate.relation() {
                            Declaration::EDB(inner) => {
                                RelationBinding::edb(inner.id(), metadata.alias)
                            }
                            Declaration::IDB(inner) => {
                                RelationBinding::idb(inner.id(), metadata.alias)
                            }
                        };

                        match attribute_value {
                            ColumnValue::Literal(literal) => {
                                let formula = Formula::equality(
                                    Term::Attribute(attribute_id, binding),
                                    Term::Literal(literal.clone()),
                                );

                                formulae.push(formula);
                            }
                            ColumnValue::Binding(var) => match metadata.bindings.get(&var.id()) {
                                None => (),
                                Some(Term::Attribute(_, attribute_binding))
                                    if *attribute_binding == binding => {}
                                Some(bound_value) => {
                                    let formula = Formula::equality(
                                        Term::Attribute(attribute_id, binding),
                                        bound_value.clone(),
                                    );

                                    formulae.push(formula);
                                }
                            },
                        }
                    }

                    if let Declaration::IDB(inner) = &**predicate.relation() {
                        if inner.id() == rule.head()
                            && projection_variables.iter().all(|&v| metadata.is_bound(v))
                        {
                            formulae.push(Formula::not_in(
                                Vec::from_iter(projection_attributes.clone()),
                                RelationRef::idb(rule.head(), RelationVersion::Total),
                            ))
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
                        let attributes = negation.args().iter().map(|(k, v)| match v {
                            ColumnValue::Literal(literal) => (*k, Term::Literal(literal.clone())),
                            ColumnValue::Binding(var) => {
                                (*k, metadata.bindings.get(&var.id()).unwrap().clone())
                            }
                        });

                        let relation_ref = match &**negation.relation() {
                            Declaration::EDB(inner) => {
                                RelationRef::edb(inner.id(), RelationVersion::Total)
                            }
                            Declaration::IDB(inner) => {
                                RelationRef::idb(inner.id(), RelationVersion::Total)
                            }
                        };

                        formulae.push(Formula::not_in(attributes, relation_ref))
                    }

                    let version = if mask & (1 << i) != 0 {
                        RelationVersion::Delta
                    } else {
                        RelationVersion::Total
                    };

                    let relation_ref = match &**predicate.relation() {
                        Declaration::EDB(inner) => RelationRef::edb(inner.id(), version),
                        Declaration::IDB(inner) => RelationRef::idb(inner.id(), version),
                    };

                    previous = Operation::Search(Search::new(
                        relation_ref,
                        metadata.alias,
                        formulae,
                        previous,
                    ));
                }
                BodyTerm::GetLink(inner) => {
                    if mask & (1 << i) != 0 {
                        skip = true;

                        break;
                    };

                    let cid_term = match inner.cid() {
                        CidValue::Cid(cid) => Term::Literal(Value::Cid(cid)),
                        CidValue::Var(var) => bindings.get(&var.id()).unwrap().clone(),
                    };

                    let link_value = match inner.link_value() {
                        CidValue::Cid(cid) => Term::Literal(Value::Cid(cid)),
                        CidValue::Var(var) => bindings.get(&var.id()).unwrap().clone(),
                    };

                    previous = Operation::GetLink(GetLink::new(
                        cid_term,
                        inner.link_id(),
                        link_value,
                        // TODO: move unification logic into formula
                        vec![],
                        previous,
                    ));
                }
                BodyTerm::Negation(_) => unreachable!("Only iterating through positive terms"),
            };
        }

        if !skip {
            statements.push(Statement::Insert(Insert::new(previous, false)));
        }
    }

    Ok(statements)
}

pub fn stratify(program: &Program) -> Result<Vec<Stratum>> {
    let mut clauses_by_relation = im::HashMap::<RelationId, im::Vector<Clause>>::default();

    for clause in program.clauses() {
        clauses_by_relation = clauses_by_relation.alter(
            |old| match old {
                Some(clauses) => {
                    let mut new = clauses;
                    new.push_back(clause.clone());

                    Some(new)
                }
                None => Some(im::vector![clause.clone()]),
            },
            clause.head(),
        );
    }

    let mut edg = DiGraph::<Node, Polarity>::default();
    let mut nodes = im::HashMap::<Node, NodeIndex>::default();

    for clause in program.clauses() {
        nodes = nodes.alter(
            |old| match old {
                Some(node) => Some(node),
                None => Some(edg.add_node(Node::IDB(clause.head()))),
            },
            Node::IDB(clause.head()),
        );

        for dependency in clause.depends_on() {
            nodes = nodes.alter(
                |old| match old {
                    Some(id) => Some(id),
                    None => Some(edg.add_node(dependency.to())),
                },
                dependency.to(),
            );

            nodes = nodes.alter(
                |old| match old {
                    Some(id) => Some(id),
                    None => Some(edg.add_node(dependency.from())),
                },
                dependency.from(),
            );

            let to = nodes.get(&dependency.to()).unwrap();
            let from = nodes.get(&dependency.from()).unwrap();

            edg.add_edge(*from, *to, dependency.polarity());
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
            let mut relations: HashSet<RelationId> = HashSet::default();
            let mut clauses: Vec<Clause> = Vec::default();

            for i in nodes {
                if let Some(Node::IDB(id)) = edg.node_weight(*i) {
                    relations.insert(*id);

                    for clause in clauses_by_relation.get(id).cloned().unwrap_or_default() {
                        clauses.push(clause);
                    }
                }
            }

            Stratum::new(
                relations,
                clauses,
                nodes.len() > 1 || edg.contains_edge(nodes[0], nodes[0]),
            )
        })
        .rev()
        .collect())
}

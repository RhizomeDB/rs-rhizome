use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::{ColId, RelationId},
    ram::{
        self, AliasId, Exit, Formula, Insert, Loop, Merge, Operation, Project, Purge, Reduce,
        RelationBinding, RelationRef, RelationVersion, Search, Sinks, Sources, Statement, Swap,
        Term,
    },
    relation::{Edb, Idb},
    value::Val,
    var::Var,
};

use super::ast::{
    body_term::BodyTerm,
    cid_value::CidValue,
    clause::Clause,
    declaration::{Declaration, InnerDeclaration},
    dependency::{Node, Polarity},
    fact::Fact,
    program::Program,
    rule::Rule,
    stratum::Stratum,
};

pub(crate) fn lower_to_ram(program: &Program) -> Result<ram::program::Program> {
    let mut inputs: Vec<&InnerDeclaration<Edb>> = Vec::default();
    let mut outputs: Vec<&InnerDeclaration<Idb>> = Vec::default();
    let mut statements: Vec<Statement> = Vec::default();

    for declaration in program.declarations() {
        match &**declaration {
            Declaration::Edb(inner) => {
                inputs.push(inner);
            }
            Declaration::Idb(inner) => {
                outputs.push(inner);
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

    let input_schemas = inputs.into_iter().map(|d| d.schema()).collect();
    let output_schemas = outputs.into_iter().map(|d| d.schema()).collect();

    let statements = statements.into_iter().map(Arc::new).collect();

    Ok(ram::program::Program::new(
        input_schemas,
        output_schemas,
        statements,
    ))
}

pub(crate) fn lower_stratum_to_ram(
    stratum: &Stratum<'_>,
    program: &Program,
) -> Result<Vec<Statement>> {
    let mut statements: Vec<Statement> = Vec::default();

    if stratum.is_recursive() {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(fact)?;

            statements.push(lowered);
        }

        // Partition the stratum's rules based on whether they depend on relations
        // that change during this stratum
        let (dynamic_rules, static_rules): (Vec<&Rule>, Vec<&Rule>) =
            stratum.rules().iter().partition(|r| {
                r.rel_predicate_terms()
                    .iter()
                    .any(|p| match &*p.relation() {
                        Declaration::Edb(_) => false,
                        Declaration::Idb(inner) => stratum.relations().contains(&inner.id()),
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

        let loop_body: Vec<Arc<Statement>> = loop_body.into_iter().map(Arc::new).collect();

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
            let lowered = lower_fact_to_ram(fact)?;

            statements.push(lowered);
        }

        // Evaluate all rules, inserting into Delta
        for rule in stratum.rules() {
            let mut lowered = lower_rule_to_ram(rule, stratum, program, RelationVersion::Delta)?;

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

pub(crate) fn lower_fact_to_ram(fact: &Fact) -> Result<Statement> {
    let cols = fact
        .args()
        .iter()
        .map(|(k, v)| (*k, Term::Lit(Arc::clone(v))));

    Ok(Statement::Insert(Insert::new(
        Operation::Project(Project::new(
            cols,
            RelationRef::idb(fact.head(), RelationVersion::Delta),
        )),
        true,
    )))
}

struct TermMetadata {
    alias: Option<AliasId>,
    bindings: im::HashMap<Var, Term>,
}

impl TermMetadata {
    fn new(alias: Option<AliasId>, bindings: im::HashMap<Var, Term>) -> Self {
        Self { alias, bindings }
    }

    fn is_bound(&self, var: &Var) -> bool {
        self.bindings.contains_key(var)
    }
}

pub(crate) fn lower_rule_to_ram(
    rule: &Rule,
    _stratum: &Stratum<'_>,
    _program: &Program,
    version: RelationVersion,
) -> Result<Vec<Statement>> {
    let mut next_alias = im::HashMap::<RelationId, AliasId>::default();
    let mut bindings = im::HashMap::<Var, Term>::default();
    let mut term_metadata = Vec::<(&BodyTerm, TermMetadata)>::default();

    for body_term in rule.body() {
        match body_term {
            BodyTerm::RelPredicate(predicate) => {
                let alias = next_alias.get(&predicate.relation().id()).copied();

                next_alias = next_alias.update_with(
                    predicate.relation().id(),
                    AliasId::default(),
                    |old, _| old.next(),
                );

                for (col_id, col_val) in predicate.args() {
                    match col_val {
                        ColVal::Lit(_) => continue,
                        ColVal::Binding(var) if !bindings.contains_key(var) => {
                            let col_binding = match &*predicate.relation() {
                                Declaration::Edb(inner) => RelationBinding::edb(inner.id(), alias),
                                Declaration::Idb(inner) => RelationBinding::idb(inner.id(), alias),
                            };

                            bindings.insert(*var, Term::Col(*col_id, col_binding))
                        }
                        _ => continue,
                    };
                }

                term_metadata.push((body_term, TermMetadata::new(alias, bindings.clone())));
            }
            BodyTerm::Negation(_) => continue,
            BodyTerm::GetLink(inner) => {
                if let CidValue::Var(val_var) = inner.link_value() {
                    if !bindings.contains_key(&val_var) {
                        match inner.cid() {
                            CidValue::Cid(cid) => {
                                bindings.insert(
                                    val_var,
                                    Term::Link(
                                        inner.link_id(),
                                        Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                                    ),
                                );
                            }
                            CidValue::Var(var) => {
                                if let Some(term) = bindings.get(&var) {
                                    bindings.insert(
                                        val_var,
                                        Term::Link(inner.link_id(), Box::new(term.clone())),
                                    );
                                } else {
                                    return error(Error::ClauseNotDomainIndependent(var.id()));
                                }
                            }
                        };
                    }
                }
            }
            BodyTerm::VarPredicate(_) => continue,
            BodyTerm::Reduce(inner) => {
                if !bindings.contains_key(inner.target()) {
                    let alias = next_alias.get(&inner.relation().id()).copied();

                    next_alias = next_alias.update_with(
                        inner.relation().id(),
                        AliasId::default(),
                        |old, _| old.next(),
                    );

                    let rel_binding = match &*inner.relation() {
                        Declaration::Edb(inner) => RelationBinding::edb(inner.id(), alias),
                        Declaration::Idb(inner) => RelationBinding::idb(inner.id(), alias),
                    };

                    bindings.insert(*inner.target(), Term::Agg(*inner.target(), rel_binding));

                    term_metadata.push((body_term, TermMetadata::new(alias, bindings.clone())));
                }
            }
        }
    }

    let projection_cols: im::HashMap<ColId, Term> = rule
        .args()
        .iter()
        .map(|(k, v)| match v {
            ColVal::Lit(c) => (*k, Term::Lit(Arc::clone(c))),
            ColVal::Binding(v) => (*k, bindings.get(v).unwrap().clone()),
        })
        .collect();

    let projection_vars: Vec<Var> = rule
        .args()
        .iter()
        .filter_map(|(_, v)| match *v {
            ColVal::Lit(_) => None,
            ColVal::Binding(var) => Some(var),
        })
        .collect();

    let mut statements: Vec<Statement> = Vec::default();

    // We use a bitmask to represent all of the possible rewrites of the rule under
    // semi-naive evaluation, i.e. those where at least one rel_predicate searches
    // against a delta relation, rather than total.
    let rewrite_count = (1 << term_metadata.len()) - 1;

    for offset in 0..rewrite_count {
        // bitmask of dynamic rel_predicate versions (1 => delta, 0 => total)
        let mask = (1 << term_metadata.len()) - 1 - offset;

        let mut negation_terms = rule.negation_terms();
        let mut get_link_terms = rule.get_link_terms();
        let mut var_predicate_terms = rule.var_predicate_terms();

        let mut previous = Operation::Project(Project::new(
            projection_cols.clone(),
            RelationRef::idb(rule.head(), version),
        ));

        for (i, (body_term, metadata)) in term_metadata.iter().rev().enumerate() {
            let mut formulae = Vec::default();

            // TODO: Add this once, after all projection vars are bound
            if projection_vars.iter().all(|&v| metadata.is_bound(&v)) {
                formulae.push(Formula::not_in(
                    Vec::from_iter(projection_cols.clone()),
                    RelationRef::idb(rule.head(), RelationVersion::Total),
                ))
            }

            match body_term {
                BodyTerm::RelPredicate(predicate) => {
                    for (&col_id, col_val) in predicate.args() {
                        let binding = match &*predicate.relation() {
                            Declaration::Edb(inner) => {
                                RelationBinding::edb(inner.id(), metadata.alias)
                            }
                            Declaration::Idb(inner) => {
                                RelationBinding::idb(inner.id(), metadata.alias)
                            }
                        };

                        match col_val {
                            ColVal::Lit(val) => {
                                let formula = Formula::equality(
                                    Term::Col(col_id, binding),
                                    Term::Lit(Arc::clone(val)),
                                );

                                formulae.push(formula);
                            }
                            ColVal::Binding(var) => {
                                if let Some(bound) = metadata.bindings.get(var) {
                                    match bound {
                                        Term::Col(_, col_binding) if *col_binding == binding => {}
                                        bound_value => {
                                            let formula = Formula::equality(
                                                Term::Col(col_id, binding),
                                                bound_value.clone(),
                                            );

                                            formulae.push(formula);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = negation_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&bindings));

                    negation_terms = unsatisfied;

                    for negation in satisfied {
                        let cols = negation.args().iter().map(|(k, v)| match v {
                            ColVal::Lit(val) => (*k, Term::Lit(Arc::clone(val))),
                            ColVal::Binding(var) => {
                                (*k, metadata.bindings.get(var).unwrap().clone())
                            }
                        });

                        let relation_ref = match &*negation.relation() {
                            Declaration::Edb(inner) => {
                                RelationRef::edb(inner.id(), RelationVersion::Total)
                            }
                            Declaration::Idb(inner) => {
                                RelationRef::idb(inner.id(), RelationVersion::Total)
                            }
                        };

                        formulae.push(Formula::not_in(cols, relation_ref))
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = get_link_terms
                        .into_iter()
                        .partition(|term| match term.cid() {
                            CidValue::Cid(_) => true,
                            CidValue::Var(var) => bindings.contains_key(&var),
                        });

                    get_link_terms = unsatisfied;

                    for term in satisfied {
                        let cid_term = match term.cid() {
                            CidValue::Cid(cid) => Term::Lit(Arc::new(Val::Cid(cid))),
                            CidValue::Var(var) => Term::Link(
                                term.link_id(),
                                Box::new(metadata.bindings.get(&var).unwrap().clone()),
                            ),
                        };

                        let val_term = match term.link_value() {
                            CidValue::Cid(cid) => Term::Link(
                                term.link_id(),
                                Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                            ),
                            CidValue::Var(var) => metadata.bindings.get(&var).unwrap().clone(),
                        };

                        formulae.push(Formula::equality(cid_term, val_term));
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = var_predicate_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&bindings));

                    var_predicate_terms = unsatisfied;

                    for term in satisfied {
                        let var_terms = term
                            .vars()
                            .iter()
                            .map(|var| metadata.bindings.get(var).unwrap().clone())
                            .collect();

                        formulae.push(Formula::predicate(var_terms, term.f()));
                    }

                    let version = if mask & (1 << i) != 0 {
                        RelationVersion::Delta
                    } else {
                        RelationVersion::Total
                    };

                    let relation_ref = match &*predicate.relation() {
                        Declaration::Edb(inner) => RelationRef::edb(inner.id(), version),
                        Declaration::Idb(inner) => RelationRef::idb(inner.id(), version),
                    };

                    previous = Operation::Search(Search::new(
                        relation_ref,
                        metadata.alias,
                        formulae,
                        previous,
                    ));
                }
                BodyTerm::GetLink(_) => unreachable!("Only iterating through positive terms"),
                BodyTerm::Negation(_) => unreachable!("Only iterating through positive terms"),
                BodyTerm::VarPredicate(_) => unreachable!("Only iterating through positive terms"),
                BodyTerm::Reduce(agg) => {
                    let rel_binding = match &*agg.relation() {
                        Declaration::Edb(inner) => RelationBinding::edb(inner.id(), metadata.alias),
                        Declaration::Idb(inner) => RelationBinding::idb(inner.id(), metadata.alias),
                    };

                    let mut args = Vec::default();
                    let mut group_by_cols = HashMap::default();
                    for (col_id, col_val) in agg.group_by_cols() {
                        if let Some(term) = match col_val {
                            ColVal::Lit(lit) => Some(Term::Lit(Arc::clone(lit))),
                            ColVal::Binding(var) => {
                                if let Some(term) = bindings.get(var) {
                                    if agg.vars().contains(var) {
                                        args.push(term.clone());
                                    }

                                    Some(term.clone())
                                } else if agg.vars().contains(var) {
                                    args.push(Term::Col(*col_id, rel_binding));

                                    None
                                } else {
                                    return error(Error::ClauseNotDomainIndependent(var.id()));
                                }
                            }
                        } {
                            group_by_cols.insert(*col_id, term);
                        }
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = negation_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&bindings));

                    negation_terms = unsatisfied;

                    for negation in satisfied {
                        let cols = negation.args().iter().map(|(k, v)| match v {
                            ColVal::Lit(val) => (*k, Term::Lit(Arc::clone(val))),
                            ColVal::Binding(var) => {
                                (*k, metadata.bindings.get(var).unwrap().clone())
                            }
                        });

                        let relation_ref = match &*negation.relation() {
                            Declaration::Edb(inner) => {
                                RelationRef::edb(inner.id(), RelationVersion::Total)
                            }
                            Declaration::Idb(inner) => {
                                RelationRef::idb(inner.id(), RelationVersion::Total)
                            }
                        };

                        formulae.push(Formula::not_in(cols, relation_ref))
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = get_link_terms
                        .into_iter()
                        .partition(|term| match term.cid() {
                            CidValue::Cid(_) => true,
                            CidValue::Var(var) => bindings.contains_key(&var),
                        });

                    get_link_terms = unsatisfied;

                    for term in satisfied {
                        let cid_term = match term.cid() {
                            CidValue::Cid(cid) => Term::Lit(Arc::new(Val::Cid(cid))),
                            CidValue::Var(var) => Term::Link(
                                term.link_id(),
                                Box::new(metadata.bindings.get(&var).unwrap().clone()),
                            ),
                        };

                        let val_term = match term.link_value() {
                            CidValue::Cid(cid) => Term::Link(
                                term.link_id(),
                                Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                            ),
                            CidValue::Var(var) => metadata.bindings.get(&var).unwrap().clone(),
                        };

                        formulae.push(Formula::equality(cid_term, val_term));
                    }

                    let (satisfied, unsatisfied): (Vec<_>, Vec<_>) = var_predicate_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&bindings));

                    var_predicate_terms = unsatisfied;

                    for term in satisfied {
                        let var_terms = term
                            .vars()
                            .iter()
                            .map(|var| metadata.bindings.get(var).unwrap().clone())
                            .collect();

                        formulae.push(Formula::predicate(var_terms, term.f()));
                    }

                    let version = if mask & (1 << i) != 0 {
                        RelationVersion::Delta
                    } else {
                        RelationVersion::Total
                    };

                    let relation_ref = match &*agg.relation() {
                        Declaration::Edb(inner) => RelationRef::edb(inner.id(), version),
                        Declaration::Idb(inner) => RelationRef::idb(inner.id(), version),
                    };

                    previous = Operation::Reduce(Reduce::new(
                        args,
                        agg.init().clone(),
                        agg.f(),
                        *agg.target(),
                        group_by_cols,
                        relation_ref,
                        metadata.alias,
                        formulae,
                        previous,
                    ));
                }
            };
        }

        statements.push(Statement::Insert(Insert::new(previous, false)));
    }

    Ok(statements)
}

pub(crate) fn stratify(program: &Program) -> Result<Vec<Stratum<'_>>> {
    let mut clauses_by_relation = im::HashMap::<RelationId, im::Vector<&Clause>>::default();

    for clause in program.clauses() {
        clauses_by_relation = clauses_by_relation.alter(
            |old| match old {
                Some(clauses) => {
                    let mut new = clauses;
                    new.push_back(clause);

                    Some(new)
                }
                None => Some(im::vector![clause]),
            },
            clause.head(),
        );
    }

    let mut edg = DiGraph::<Node, Polarity>::default();
    let mut nodes = im::HashMap::<Node, NodeIndex>::default();

    for clause in program.clauses() {
        nodes
            .entry(Node::Idb(clause.head()))
            .or_insert_with(|| edg.add_node(Node::Idb(clause.head())));

        for dependency in clause.depends_on() {
            nodes
                .entry(dependency.to())
                .or_insert_with(|| edg.add_node(dependency.to()));

            nodes
                .entry(dependency.from())
                .or_insert_with(|| edg.add_node(dependency.from()));

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
            let mut clauses: Vec<&Clause> = Vec::default();

            for i in nodes {
                if let Some(Node::Idb(id)) = edg.node_weight(*i) {
                    relations.insert(*id);

                    if let Some(by_relation) = clauses_by_relation.get(id) {
                        for clause in by_relation {
                            clauses.push(*clause);
                        }
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

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
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
    fact::traits::{EDBFact, IDBFact},
    id::{ColId, RelationId},
    ram::{
        self, AliasId, ExitBuilder, Formula, Insert, Loop, Merge, MergeRelations, NotInRelation,
        Operation, Project, Purge, PurgeRelation, Reduce, ReduceRelation, RelationVersion, Search,
        SearchRelation, SinksBuilder, SourcesBuilder, Statement, Swap, Term,
    },
    relation::{Relation, Source},
    value::Val,
    var::Var,
};

use super::ast::{
    body_term::BodyTerm,
    cid_value::CidValue,
    clause::Clause,
    declaration::Declaration,
    dependency::{Node, Polarity},
    fact::Fact,
    program::Program,
    rule::Rule,
    stratum::Stratum,
};

pub(crate) fn lower_to_ram<EF, IF, ER, IR>(
    program: &Program,
) -> Result<ram::program::Program<EF, IF, ER, IR>>
where
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    let mut edb = HashMap::default();
    let mut idb = HashMap::default();

    let mut inputs: Vec<&Declaration> = Vec::default();
    let mut outputs: Vec<&Declaration> = Vec::default();
    let mut statements: Vec<Statement<EF, IF, ER, IR>> = Vec::default();

    for declaration in program.declarations() {
        match declaration.source() {
            Source::Edb => {
                inputs.push(declaration);

                edb.insert((declaration.id(), RelationVersion::New), Arc::default());

                edb.insert((declaration.id(), RelationVersion::Delta), Arc::default());

                edb.insert((declaration.id(), RelationVersion::Total), Arc::default());
            }
            Source::Idb => {
                outputs.push(declaration);

                idb.insert((declaration.id(), RelationVersion::New), Arc::default());

                idb.insert((declaration.id(), RelationVersion::Delta), Arc::default());

                idb.insert((declaration.id(), RelationVersion::Total), Arc::default());
            }
        }
    }

    // Run sources for each input
    if !inputs.is_empty() {
        let mut sources_builder = SourcesBuilder::default();

        for input in &inputs {
            let id = input.id();
            let relation = Arc::clone(
                edb.get(&(id, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            sources_builder.add_relation(id, relation);
        }

        statements.push(Statement::Sources(sources_builder.finalize()));
    }

    for stratum in &stratify(program)? {
        let mut lowered = lower_stratum_to_ram(stratum, program, &edb, &idb)?;

        statements.append(&mut lowered);
    }

    // Merge all newly received input fact into Total and then purge Delta
    for input in &inputs {
        let id = input.id();

        let delta_relation = Arc::clone(
            edb.get(&(id, RelationVersion::Delta))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        let total_relation = Arc::clone(
            edb.get(&(id, RelationVersion::Total))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        let merge = Merge::new(
            id,
            RelationVersion::Delta,
            id,
            RelationVersion::Total,
            MergeRelations::Edb(Arc::clone(&delta_relation), total_relation),
        );

        statements.push(Statement::Merge(merge));

        statements.push(Statement::Purge(Purge::new(
            id,
            RelationVersion::Delta,
            PurgeRelation::Edb(delta_relation),
        )));
    }

    // Purge Delta for all outputs
    for output in &outputs {
        let id = output.id();
        let relation = Arc::clone(
            idb.get(&(id, RelationVersion::Delta))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        statements.push(Statement::Purge(Purge::new(
            id,
            RelationVersion::Delta,
            PurgeRelation::Idb(relation),
        )));
    }

    let statements = statements.into_iter().map(Arc::new).collect();

    Ok(ram::program::Program::new(statements))
}

pub(crate) fn lower_stratum_to_ram<EF, IF, ER, IR>(
    stratum: &Stratum<'_>,
    program: &Program,
    edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<Vec<Statement<EF, IF, ER, IR>>>
where
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    let mut statements: Vec<Statement<EF, IF, ER, IR>> = Vec::default();

    if stratum.is_recursive() {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(fact, edb, idb)?;

            statements.push(lowered);
        }

        // Partition the stratum's rules based on whether they depend on relations
        // that change during this stratum
        let (dynamic_rules, static_rules): (Vec<&Rule>, Vec<&Rule>) =
            stratum.rules().iter().partition(|r| {
                r.rel_predicate_terms()
                    .iter()
                    .any(|p| match p.relation().source() {
                        Source::Edb => false,
                        Source::Idb => stratum.relations().contains(&p.relation().id()),
                    })
            });

        // Evaluate static rules out of the loop
        for rule in &static_rules {
            let mut lowered =
                lower_rule_to_ram(rule, stratum, program, RelationVersion::Delta, edb, idb)?;

            statements.append(&mut lowered);
        }

        // Merge the output of the static rules into total
        for relation in HashSet::<RelationId>::from_iter(static_rules.iter().map(|r| r.head())) {
            let from_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                relation,
                RelationVersion::Delta,
                relation,
                RelationVersion::Total,
                MergeRelations::Idb(Arc::clone(&from_relation), into_relation),
            );

            statements.push(Statement::Merge(merge));
        }

        let mut loop_body: Vec<Statement<EF, IF, ER, IR>> = Vec::default();

        // Purge new, computed during the last loop iteration
        for &id in stratum.relations() {
            let relation = Arc::clone(
                idb.get(&(id, RelationVersion::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let statement = Statement::Purge(Purge::new(
                id,
                RelationVersion::New,
                PurgeRelation::Idb(relation),
            ));

            loop_body.push(statement);
        }

        // Evaluate dynamic rules within the loop, inserting into new
        for rule in &dynamic_rules {
            let mut lowered =
                lower_rule_to_ram(rule, stratum, program, RelationVersion::New, edb, idb)?;

            loop_body.append(&mut lowered);
        }

        // Run sinks for the stratum
        let mut sinks_builder = SinksBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                idb.get(&(id, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            sinks_builder.add_relation(id, relation);
        }

        if !sinks_builder.relations.is_empty() {
            loop_body.push(Statement::Sinks(sinks_builder.finalize()));
        }

        // Exit the loop if all of the dynamic relations have reached a fixed point
        let mut exit_builder = ExitBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                idb.get(&(id, RelationVersion::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            exit_builder.add_relation(id, RelationVersion::New, relation);
        }

        loop_body.push(Statement::Exit(exit_builder.finalize()));

        // Merge new into total, then swap new and delta
        for &relation in stratum.relations() {
            // Merge the output of the static rules into total
            let from_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                relation,
                RelationVersion::New,
                relation,
                RelationVersion::Total,
                MergeRelations::Idb(Arc::clone(&from_relation), into_relation),
            );

            loop_body.push(Statement::Merge(merge));

            // Swap new and delta
            let left_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let right_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let swap = Swap::new(
                relation,
                RelationVersion::New,
                relation,
                RelationVersion::Delta,
                left_relation,
                right_relation,
            );

            loop_body.push(Statement::Swap(swap));
        }

        let loop_body: Vec<Arc<Statement<EF, IF, ER, IR>>> =
            loop_body.into_iter().map(Arc::new).collect();

        statements.push(Statement::Loop(Loop::new(loop_body)));

        // Merge total into delta for subsequent strata
        // TODO: this seems wrong and will lead to duplicate work across epochs. Will likely need to
        // use the lattice based timestamps to resolve that.
        for &relation in stratum.relations() {
            let from_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                relation,
                RelationVersion::Total,
                relation,
                RelationVersion::Delta,
                MergeRelations::Idb(Arc::clone(&from_relation), into_relation),
            );

            statements.push(Statement::Merge(merge));
        }
    } else {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(fact, edb, idb)?;

            statements.push(lowered);
        }

        // Evaluate all rules, inserting into Delta
        for rule in stratum.rules() {
            let mut lowered =
                lower_rule_to_ram(rule, stratum, program, RelationVersion::Delta, edb, idb)?;

            statements.append(&mut lowered);
        }

        // Merge rules from Delta into Total
        for &relation in stratum.relations() {
            let from_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                idb.get(&(relation, RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                relation,
                RelationVersion::Delta,
                relation,
                RelationVersion::Total,
                MergeRelations::Idb(Arc::clone(&from_relation), into_relation),
            );

            statements.push(Statement::Merge(merge));
        }

        // Run sinks for the stratum
        let mut sinks_builder = SinksBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                idb.get(&(id, RelationVersion::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            sinks_builder.add_relation(id, relation);
        }

        if !sinks_builder.relations.is_empty() {
            statements.push(Statement::Sinks(sinks_builder.finalize()));
        }
    };

    Ok(statements)
}

pub(crate) fn lower_fact_to_ram<EF, IF, ER, IR>(
    fact: &Fact,
    _edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<Statement<EF, IF, ER, IR>>
where
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    let cols = fact
        .args()
        .iter()
        .map(|(k, v)| (*k, Term::Lit(Arc::clone(v))));

    let relation = Arc::clone(
        idb.get(&(fact.head(), RelationVersion::Delta))
            .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
    );

    Ok(Statement::Insert(Insert::new(
        Operation::Project(Project::new(
            fact.head(),
            RelationVersion::Delta,
            cols,
            relation,
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

pub(crate) fn lower_rule_to_ram<EF, IF, ER, IR>(
    rule: &Rule,
    _stratum: &Stratum<'_>,
    _program: &Program,
    version: RelationVersion,
    edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<Vec<Statement<EF, IF, ER, IR>>>
where
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
    EF: EDBFact,
    IF: IDBFact,
{
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
                        ColVal::Binding(var) if !bindings.contains_key(var) => bindings
                            .insert(*var, Term::Col(predicate.relation().id(), alias, *col_id)),
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

                    bindings.insert(
                        *inner.target(),
                        Term::Agg(inner.relation().id(), alias, *inner.target()),
                    );

                    term_metadata.push((body_term, TermMetadata::new(alias, bindings.clone())));
                }
            }
        }
    }

    let mut projection_vars = Vec::<Var>::default();
    let mut projection_cols = im::HashMap::<ColId, Term>::default();
    for (k, v) in rule.args() {
        if let ColVal::Binding(var) = v {
            projection_vars.push(*var);
        }

        let term = match v {
            ColVal::Lit(c) => Term::Lit(Arc::clone(c)),
            ColVal::Binding(v) => bindings
                .get(v)
                .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
                .clone(),
        };

        projection_cols.insert(*k, term);
    }

    let mut statements: Vec<Statement<EF, IF, ER, IR>> = Vec::default();

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

        let relation = Arc::clone(
            idb.get(&(rule.head(), version))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        let mut previous = Operation::Project(Project::new(
            rule.head(),
            version,
            projection_cols.clone(),
            relation,
        ));

        for (i, (body_term, metadata)) in term_metadata.iter().rev().enumerate() {
            let mut formulae = Vec::default();

            // TODO: Add this once, after all projection vars are bound
            if projection_vars.iter().all(|&v| metadata.is_bound(&v)) {
                let not_in_relation = NotInRelation::Idb(Arc::clone(
                    idb.get(&(rule.head(), RelationVersion::Total))
                        .ok_or_else(|| {
                            Error::InternalRhizomeError("relation not found".to_owned())
                        })?,
                ));

                formulae.push(Formula::not_in(
                    rule.head(),
                    RelationVersion::Total,
                    Vec::from_iter(projection_cols.clone()),
                    not_in_relation,
                ))
            }

            match body_term {
                BodyTerm::RelPredicate(predicate) => {
                    for (&col_id, col_val) in predicate.args() {
                        match col_val {
                            ColVal::Lit(val) => {
                                let formula = Formula::equality(
                                    Term::Col(predicate.relation().id(), metadata.alias, col_id),
                                    Term::Lit(Arc::clone(val)),
                                );

                                formulae.push(formula);
                            }
                            ColVal::Binding(var) => {
                                if let Some(bound) = metadata.bindings.get(var) {
                                    match bound {
                                        Term::Col(rel_id, alias, _)
                                            if *rel_id == predicate.relation().id()
                                                && *alias == metadata.alias => {}
                                        bound_value => {
                                            let formula = Formula::equality(
                                                Term::Col(
                                                    predicate.relation().id(),
                                                    metadata.alias,
                                                    col_id,
                                                ),
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
                        let mut negation_cols = im::HashMap::<ColId, Term>::default();
                        for (k, v) in negation.args() {
                            let term = match v {
                                ColVal::Lit(val) => Term::Lit(Arc::clone(val)),
                                ColVal::Binding(var) => metadata
                                    .bindings
                                    .get(var)
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("binding not found".to_owned())
                                    })?
                                    .clone(),
                            };

                            negation_cols.insert(*k, term);
                        }

                        let not_in_relation = match negation.relation().source() {
                            Source::Edb => NotInRelation::Edb(Arc::clone(
                                edb.get(&(negation.relation().id(), RelationVersion::Total))
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("relation not found".to_owned())
                                    })?,
                            )),

                            Source::Idb => NotInRelation::Idb(Arc::clone(
                                idb.get(&(negation.relation().id(), RelationVersion::Total))
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("relation not found".to_owned())
                                    })?,
                            )),
                        };

                        formulae.push(Formula::not_in(
                            negation.relation().id(),
                            RelationVersion::Total,
                            negation_cols,
                            not_in_relation,
                        ));
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
                                Box::new(
                                    metadata
                                        .bindings
                                        .get(&var)
                                        .ok_or_else(|| {
                                            Error::InternalRhizomeError(
                                                "binding not found".to_owned(),
                                            )
                                        })?
                                        .clone(),
                                ),
                            ),
                        };

                        let val_term = match term.link_value() {
                            CidValue::Cid(cid) => Term::Link(
                                term.link_id(),
                                Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                            ),
                            CidValue::Var(var) => metadata
                                .bindings
                                .get(&var)
                                .ok_or_else(|| {
                                    Error::InternalRhizomeError("binding not found".to_owned())
                                })?
                                .clone(),
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
                            .map(|var| {
                                Ok(metadata
                                    .bindings
                                    .get(var)
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("binding not found".to_owned())
                                    })?
                                    .clone())
                            })
                            .collect::<Result<Vec<Term>>>()?;

                        formulae.push(Formula::predicate(var_terms, term.f()));
                    }

                    let version = if mask & (1 << i) != 0 {
                        RelationVersion::Delta
                    } else {
                        RelationVersion::Total
                    };

                    let search_relation = match predicate.relation().source() {
                        Source::Edb => {
                            let relation = Arc::clone(
                                edb.get(&(predicate.relation().id(), version)).ok_or_else(
                                    || Error::InternalRhizomeError("relation not found".to_owned()),
                                )?,
                            );

                            SearchRelation::Edb(relation)
                        }
                        Source::Idb => {
                            let relation = Arc::clone(
                                idb.get(&(predicate.relation().id(), version)).ok_or_else(
                                    || Error::InternalRhizomeError("relation not found".to_owned()),
                                )?,
                            );

                            SearchRelation::Idb(relation)
                        }
                    };

                    previous = Operation::Search(Search::new(
                        predicate.relation().id(),
                        metadata.alias,
                        version,
                        search_relation,
                        formulae,
                        previous,
                    ));
                }
                BodyTerm::GetLink(_) => {
                    return error(Error::InternalRhizomeError(
                        "unexpected body term".to_owned(),
                    ));
                }
                BodyTerm::Negation(_) => {
                    return error(Error::InternalRhizomeError(
                        "unexpected body term".to_owned(),
                    ));
                }
                BodyTerm::VarPredicate(_) => {
                    return error(Error::InternalRhizomeError(
                        "unexpected body term".to_owned(),
                    ));
                }
                BodyTerm::Reduce(agg) => {
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
                                    args.push(Term::Col(
                                        agg.relation().id(),
                                        metadata.alias,
                                        *col_id,
                                    ));

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
                        let mut negation_cols = im::HashMap::<ColId, Term>::default();
                        for (k, v) in negation.args() {
                            let term = match v {
                                ColVal::Lit(val) => Term::Lit(Arc::clone(val)),
                                ColVal::Binding(var) => metadata
                                    .bindings
                                    .get(var)
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("binding not found".to_owned())
                                    })?
                                    .clone(),
                            };

                            negation_cols.insert(*k, term);
                        }

                        let not_in_relation = match negation.relation().source() {
                            Source::Edb => NotInRelation::Edb(Arc::clone(
                                edb.get(&(negation.relation().id(), RelationVersion::Total))
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("relation not found".to_owned())
                                    })?,
                            )),
                            Source::Idb => NotInRelation::Idb(Arc::clone(
                                idb.get(&(negation.relation().id(), RelationVersion::Total))
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("relation not found".to_owned())
                                    })?,
                            )),
                        };

                        formulae.push(Formula::not_in(
                            negation.relation().id(),
                            RelationVersion::Total,
                            negation_cols,
                            not_in_relation,
                        ));
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
                                Box::new(
                                    metadata
                                        .bindings
                                        .get(&var)
                                        .ok_or_else(|| {
                                            Error::InternalRhizomeError(
                                                "binding not found".to_owned(),
                                            )
                                        })?
                                        .clone(),
                                ),
                            ),
                        };

                        let val_term = match term.link_value() {
                            CidValue::Cid(cid) => Term::Link(
                                term.link_id(),
                                Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                            ),
                            CidValue::Var(var) => metadata
                                .bindings
                                .get(&var)
                                .ok_or_else(|| {
                                    Error::InternalRhizomeError("binding not found".to_owned())
                                })?
                                .clone(),
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
                            .map(|var| {
                                Ok(metadata
                                    .bindings
                                    .get(var)
                                    .ok_or_else(|| {
                                        Error::InternalRhizomeError("binding not found".to_owned())
                                    })?
                                    .clone())
                            })
                            .collect::<Result<Vec<Term>>>()?;

                        formulae.push(Formula::predicate(var_terms, term.f()));
                    }

                    let version = if mask & (1 << i) != 0 {
                        RelationVersion::Delta
                    } else {
                        RelationVersion::Total
                    };

                    let reduce_relation = match agg.relation().source() {
                        Source::Edb => {
                            let relation =
                                Arc::clone(edb.get(&(agg.relation().id(), version)).ok_or_else(
                                    || Error::InternalRhizomeError("relation not found".to_owned()),
                                )?);

                            ReduceRelation::Edb(relation)
                        }
                        Source::Idb => {
                            let relation =
                                Arc::clone(idb.get(&(agg.relation().id(), version)).ok_or_else(
                                    || Error::InternalRhizomeError("relation not found".to_owned()),
                                )?);

                            ReduceRelation::Idb(relation)
                        }
                    };

                    previous = Operation::Reduce(Reduce::new(
                        args,
                        agg.init().clone(),
                        agg.f(),
                        *agg.target(),
                        group_by_cols,
                        agg.relation().id(),
                        metadata.alias,
                        reduce_relation,
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

            let to = nodes
                .get(&dependency.to())
                .ok_or_else(|| Error::InternalRhizomeError("dependency not found".to_owned()))?;

            let from = nodes
                .get(&dependency.from())
                .ok_or_else(|| Error::InternalRhizomeError("dependency not found".to_owned()))?;

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

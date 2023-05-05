use std::{
    cmp::Ordering,
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
    id::{ColId, RelationId, VarId},
    ram::{
        self, AliasId, ExitBuilder, Formula, Insert, Loop, Merge, MergeRelations, NotInRelation,
        Operation, Project, Purge, PurgeRelation, Reduce, ReduceRelation, RelationVersion, Search,
        SearchRelation, SinksBuilder, SourcesBuilder, Statement, Swap, Term,
    },
    relation::{Relation, Source},
    value::Val,
};

use super::ast::{
    cid_value::CidValue,
    clause::Clause,
    declaration::Declaration,
    dependency::{Node, Polarity},
    fact::Fact,
    program::Program,
    rule::Rule,
    stratum::Stratum,
    GetLink, Negation, RelPredicate, VarPredicate,
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
        // Merge delta into total
        for &relation in stratum.relations() {
            // Merge the output of the static rules into total
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

            loop_body.push(Statement::Merge(merge));
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

        //Swap new and delta
        for &relation in stratum.relations() {
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
    let mut statements: Vec<Statement<EF, IF, ER, IR>> = Vec::default();

    for rewrite in semi_naive_rewrites(rule) {
        let operation = lower_rule_body_to_ram(
            rule.head(),
            rule.args(),
            version,
            Default::default(),
            Default::default(),
            rewrite,
            rule.negation_terms(),
            rule.get_link_terms(),
            rule.var_predicate_terms(),
            rule.reduce_terms(),
            edb,
            idb,
        )?;

        statements.push(Statement::Insert(Insert::new(operation, false)));
    }

    Ok(statements)
}

pub(crate) fn lower_rule_body_to_ram<EF, IF, ER, IR>(
    relation_id: RelationId,
    head_args: &HashMap<ColId, ColVal>,
    version: RelationVersion,
    bindings: im::HashMap<VarId, Term>,
    mut next_alias: im::HashMap<RelationId, AliasId>,
    mut rel_predicates: Vec<(&RelPredicate, RelationVersion)>,
    mut remaining_negation_terms: Vec<&Negation>,
    remaining_get_link_terms: Vec<&GetLink>,
    mut remaining_var_predicate_terms: Vec<&VarPredicate>,
    remaining_reduce_terms: Vec<&super::ast::body_term::Reduce>,
    edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<ram::Operation<EF, IF, ER, IR>>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    let operation = match rel_predicates.pop() {
        None => {
            let mut bindings = bindings;

            let (satisfied_get_link_terms, mut remaining_get_link_terms): (Vec<_>, Vec<_>) =
                remaining_get_link_terms
                    .into_iter()
                    .partition(|term| match term.cid() {
                        CidValue::Cid(_) => true,
                        CidValue::Var(var) => bindings.contains_key(&var.id()),
                    });

            for get_link in &satisfied_get_link_terms {
                if let CidValue::Var(val_var) = get_link.link_value() {
                    if !bindings.contains_key(&val_var.id()) {
                        match get_link.cid() {
                            CidValue::Cid(cid) => {
                                bindings.insert(
                                    val_var.id(),
                                    Term::Link(
                                        get_link.link_id(),
                                        Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                                    ),
                                );
                            }
                            CidValue::Var(var) => {
                                if let Some(term) = bindings.get(&var.id()) {
                                    bindings.insert(
                                        val_var.id(),
                                        Term::Link(get_link.link_id(), Box::new(term.clone())),
                                    );
                                } else {
                                    return error(Error::ClauseNotDomainIndependent(var.id()));
                                }
                            }
                        };
                    }
                }
            }

            let mut reduce_bindings = bindings.clone();
            let mut reduce_terms = vec![];

            for reduce in &remaining_reduce_terms {
                let prev_bindings = reduce_bindings.clone();
                let alias = next_alias.get(&reduce.relation().id()).copied();

                next_alias =
                    next_alias.update_with(reduce.relation().id(), AliasId::default(), |old, _| {
                        old.next()
                    });

                if !reduce_bindings.contains_key(&reduce.target().id()) {
                    reduce_bindings.insert(
                        reduce.target().id(),
                        Term::Agg(reduce.relation().id(), alias, *reduce.target()),
                    );
                }

                let (satisfied_negation_terms, remaining_negation): (Vec<_>, Vec<_>) =
                    remaining_negation_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&reduce_bindings));

                let (satisfied_get_link_terms, remaining_get_link): (Vec<_>, Vec<_>) =
                    remaining_get_link_terms
                        .into_iter()
                        .partition(|term| match term.cid() {
                            CidValue::Cid(_) => true,
                            CidValue::Var(var) => reduce_bindings.contains_key(&var.id()),
                        });

                let (satisfied_var_predicate_terms, remaining_var_predicate): (Vec<_>, Vec<_>) =
                    remaining_var_predicate_terms
                        .into_iter()
                        .partition(|n| n.is_vars_bound(&reduce_bindings));

                remaining_negation_terms = remaining_negation;
                remaining_get_link_terms = remaining_get_link;
                remaining_var_predicate_terms = remaining_var_predicate;

                for get_link in &satisfied_get_link_terms {
                    if let CidValue::Var(val_var) = get_link.link_value() {
                        if !reduce_bindings.contains_key(&val_var.id()) {
                            match get_link.cid() {
                                CidValue::Cid(cid) => {
                                    reduce_bindings.insert(
                                        val_var.id(),
                                        Term::Link(
                                            get_link.link_id(),
                                            Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                                        ),
                                    );
                                }
                                CidValue::Var(var) => {
                                    if let Some(term) = reduce_bindings.get(&var.id()) {
                                        reduce_bindings.insert(
                                            val_var.id(),
                                            Term::Link(get_link.link_id(), Box::new(term.clone())),
                                        );
                                    } else {
                                        return error(Error::ClauseNotDomainIndependent(var.id()));
                                    }
                                }
                            };
                        }
                    }
                }

                reduce_terms.push((
                    reduce,
                    alias,
                    prev_bindings,
                    satisfied_negation_terms,
                    satisfied_get_link_terms,
                    satisfied_var_predicate_terms,
                ));
            }

            debug_assert!(remaining_negation_terms.is_empty());
            debug_assert!(remaining_get_link_terms.is_empty());
            debug_assert!(remaining_var_predicate_terms.is_empty());

            let relation = idb
                .get(&(relation_id, version))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?
                .clone();

            let mut cols = im::HashMap::<ColId, Term>::default();
            for (&k, v) in head_args {
                let term = match v {
                    ColVal::Lit(c) => Term::Lit(c.clone()),
                    ColVal::Binding(v) => reduce_bindings
                        .get(&v.id())
                        .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
                        .clone(),
                };

                cols.insert(k, term);
            }

            let mut operation =
                Operation::Project(Project::new(relation_id, version, cols, relation));

            for (reduce, alias, bindings, negations, get_links, var_predicates) in
                reduce_terms.into_iter().rev()
            {
                let mut formulae = Vec::default();

                for negation in negations {
                    let formula = lower_negation_to_ram(negation, &reduce_bindings, edb, idb)?;

                    formulae.push(formula);
                }

                for get_link in get_links {
                    let formula = lower_get_link_to_ram(get_link, &reduce_bindings)?;

                    formulae.push(formula);
                }

                for var_predicate in var_predicates {
                    let formula = lower_var_predicate_to_ram(var_predicate, &reduce_bindings)?;

                    formulae.push(formula);
                }

                let mut args = Vec::default();
                let mut group_by_cols = HashMap::default();
                for (col_id, col_val) in reduce.group_by_cols() {
                    if let Some(term) = match col_val {
                        ColVal::Lit(lit) => Some(Term::Lit(lit.clone())),
                        ColVal::Binding(var) => {
                            if let Some(term) = bindings.get(&var.id()) {
                                if reduce.vars().contains(var) {
                                    args.push(term.clone());
                                }

                                Some(term.clone())
                            } else if reduce.vars().contains(var) {
                                args.push(Term::Col(reduce.relation().id(), alias, *col_id));

                                None
                            } else {
                                return error(Error::ClauseNotDomainIndependent(var.id()));
                            }
                        }
                    } {
                        group_by_cols.insert(*col_id, term);
                    }
                }

                let reduce_relation = match reduce.relation().source() {
                    Source::Edb => {
                        let relation = Arc::clone(
                            edb.get(&(reduce.relation().id(), RelationVersion::Total))
                                .ok_or_else(|| {
                                    Error::InternalRhizomeError("relation not found".to_owned())
                                })?,
                        );

                        ReduceRelation::Edb(relation)
                    }
                    Source::Idb => {
                        let relation = Arc::clone(
                            idb.get(&(reduce.relation().id(), RelationVersion::Total))
                                .ok_or_else(|| {
                                    Error::InternalRhizomeError("relation not found".to_owned())
                                })?,
                        );

                        ReduceRelation::Idb(relation)
                    }
                };

                operation = Operation::Reduce(Reduce::new(
                    args,
                    reduce.init().clone(),
                    reduce.f(),
                    *reduce.target(),
                    group_by_cols,
                    reduce.relation().id(),
                    alias,
                    reduce_relation,
                    formulae,
                    operation,
                ));
            }

            operation
        }
        Some((rel_predicate, rel_version)) => {
            let mut formulae = Vec::default();
            let mut next_bindings = bindings.clone();

            let alias = next_alias.get(&rel_predicate.relation().id()).copied();

            next_alias = next_alias.update_with(
                rel_predicate.relation().id(),
                AliasId::default(),
                |old, _| old.next(),
            );

            for (col_id, col_val) in rel_predicate.args() {
                if let ColVal::Binding(var) = col_val {
                    if !bindings.contains_key(&var.id()) {
                        next_bindings.insert(
                            var.id(),
                            Term::Col(rel_predicate.relation().id(), alias, *col_id),
                        );
                    }
                };
            }

            let (satisfied_negation_terms, remaining_negation_terms): (Vec<_>, Vec<_>) =
                remaining_negation_terms
                    .into_iter()
                    .partition(|n| n.is_vars_bound(&next_bindings));

            let (satisfied_get_link_terms, remaining_get_link_terms): (Vec<_>, Vec<_>) =
                remaining_get_link_terms
                    .into_iter()
                    .partition(|term| match term.cid() {
                        CidValue::Cid(_) => true,
                        CidValue::Var(var) => next_bindings.contains_key(&var.id()),
                    });

            let (satisfied_var_predicate_terms, remaining_var_predicate_terms): (Vec<_>, Vec<_>) =
                remaining_var_predicate_terms
                    .into_iter()
                    .partition(|n| n.is_vars_bound(&next_bindings));

            for negation in satisfied_negation_terms {
                let formula = lower_negation_to_ram(negation, &next_bindings, edb, idb)?;

                formulae.push(formula);
            }

            for get_link in satisfied_get_link_terms {
                if let CidValue::Var(val_var) = get_link.link_value() {
                    if !next_bindings.contains_key(&val_var.id()) {
                        match get_link.cid() {
                            CidValue::Cid(cid) => {
                                next_bindings.insert(
                                    val_var.id(),
                                    Term::Link(
                                        get_link.link_id(),
                                        Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                                    ),
                                );
                            }
                            CidValue::Var(var) => {
                                if let Some(term) = next_bindings.get(&var.id()) {
                                    next_bindings.insert(
                                        val_var.id(),
                                        Term::Link(get_link.link_id(), Box::new(term.clone())),
                                    );
                                } else {
                                    return error(Error::ClauseNotDomainIndependent(var.id()));
                                }
                            }
                        };
                    }
                }

                let formula = lower_get_link_to_ram(get_link, &next_bindings)?;

                formulae.push(formula);
            }

            for var_predicate in satisfied_var_predicate_terms {
                let formula = lower_var_predicate_to_ram(var_predicate, &next_bindings)?;

                formulae.push(formula);
            }

            if head_args
                .clone()
                .into_values()
                .filter_map(|v| match v {
                    ColVal::Binding(v) => Some(v),
                    _ => None,
                })
                .all(|v| next_bindings.contains_key(&v.id()))
            {
                let relation = idb
                    .get(&(relation_id, RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?
                    .clone();

                let mut cols = im::HashMap::<ColId, Term>::default();
                for (&k, v) in head_args {
                    let term = match v {
                        ColVal::Lit(c) => Term::Lit(c.clone()),
                        ColVal::Binding(v) => next_bindings
                            .get(&v.id())
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("binding not found".to_owned())
                            })?
                            .clone(),
                    };

                    cols.insert(k, term);
                }

                formulae.push(Formula::not_in(
                    relation_id,
                    RelationVersion::Total,
                    Vec::from_iter(cols.clone()),
                    NotInRelation::Idb(relation),
                ))
            }

            for (&col_id, col_val) in rel_predicate.args() {
                match col_val {
                    ColVal::Lit(val) => {
                        let formula = Formula::equality(
                            Term::Col(rel_predicate.relation().id(), alias, col_id),
                            Term::Lit(val.clone()),
                        );

                        formulae.push(formula);
                    }
                    ColVal::Binding(var) => {
                        if let Some(bound) = bindings.get(&var.id()) {
                            let formula = Formula::equality(
                                Term::Col(rel_predicate.relation().id(), alias, col_id),
                                bound.clone(),
                            );

                            formulae.push(formula);
                        }
                    }
                }
            }

            let search_relation = match rel_predicate.relation().source() {
                Source::Edb => {
                    let relation = Arc::clone(
                        edb.get(&(rel_predicate.relation().id(), rel_version))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    SearchRelation::Edb(relation)
                }
                Source::Idb => {
                    let relation = Arc::clone(
                        idb.get(&(rel_predicate.relation().id(), rel_version))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    SearchRelation::Idb(relation)
                }
            };

            Operation::Search(Search::new(
                rel_predicate.relation().id(),
                alias,
                rel_version,
                search_relation,
                formulae,
                lower_rule_body_to_ram(
                    relation_id,
                    head_args,
                    version,
                    next_bindings,
                    next_alias,
                    rel_predicates,
                    remaining_negation_terms,
                    remaining_get_link_terms,
                    remaining_var_predicate_terms,
                    remaining_reduce_terms,
                    edb,
                    idb,
                )?,
            ))
        }
    };

    Ok(operation)
}

pub(crate) fn lower_negation_to_ram<EF, IF, ER, IR>(
    negation: &Negation,
    bindings: &im::HashMap<VarId, Term>,
    edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<Formula<EF, IF, ER, IR>>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    let mut cols = im::HashMap::<ColId, Term>::default();
    for (k, v) in negation.args() {
        let term = match v {
            ColVal::Lit(val) => Term::Lit(val.clone()),
            ColVal::Binding(var) => bindings
                .get(&var.id())
                .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
                .clone(),
        };

        cols.insert(*k, term);
    }

    let not_in_relation = match negation.relation().source() {
        Source::Edb => NotInRelation::Edb(Arc::clone(
            edb.get(&(negation.relation().id(), RelationVersion::Total))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        )),

        Source::Idb => NotInRelation::Idb(Arc::clone(
            idb.get(&(negation.relation().id(), RelationVersion::Total))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        )),
    };

    Ok(Formula::not_in(
        negation.relation().id(),
        RelationVersion::Total,
        cols,
        not_in_relation,
    ))
}

pub(crate) fn lower_get_link_to_ram<EF, IF, ER, IR>(
    get_link: &GetLink,
    bindings: &im::HashMap<VarId, Term>,
) -> Result<Formula<EF, IF, ER, IR>>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    let cid_term = match get_link.cid() {
        CidValue::Cid(cid) => Term::Lit(Arc::new(Val::Cid(cid))),
        CidValue::Var(var) => Term::Link(
            get_link.link_id(),
            Box::new(
                bindings
                    .get(&var.id())
                    .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
                    .clone(),
            ),
        ),
    };

    let val_term = match get_link.link_value() {
        CidValue::Cid(cid) => Term::Link(
            get_link.link_id(),
            Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
        ),
        CidValue::Var(var) => bindings
            .get(&var.id())
            .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
            .clone(),
    };

    Ok(Formula::equality(cid_term, val_term))
}

pub(crate) fn lower_var_predicate_to_ram<EF, IF, ER, IR>(
    var_predicate: &VarPredicate,
    bindings: &im::HashMap<VarId, Term>,
) -> Result<Formula<EF, IF, ER, IR>>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    let var_terms = var_predicate
        .vars()
        .iter()
        .map(|var| {
            Ok(bindings
                .get(&var.id())
                .ok_or_else(|| Error::InternalRhizomeError("binding not found".to_owned()))?
                .clone())
        })
        .collect::<Result<Vec<Term>>>()?;

    Ok(Formula::predicate(var_terms, var_predicate.f()))
}

pub(crate) fn semi_naive_rewrites(rule: &Rule) -> Vec<Vec<(&RelPredicate, RelationVersion)>> {
    if rule.rel_predicate_terms().is_empty() {
        return vec![vec![]];
    }

    let mut rewrites: Vec<Vec<(&RelPredicate, RelationVersion)>> = vec![];

    // Use a bitmask to represent all of the possible rewrites of the rule,
    // where each rel_predicate searches against either the delta or total
    // relation. The valid semi-naive rewrites will then be the non-zero
    // bitmasks, where a 0 bit corresponds to a search against a total
    // relation, and a 1 against a delta relation.
    let rewrite_count = 1 << rule.rel_predicate_terms().len();

    for offset in 1..rewrite_count {
        let mut rewrite = vec![];

        for (i, &term) in rule.rel_predicate_terms().iter().enumerate() {
            if offset & (1 << i) == 0 {
                rewrite.push((term, RelationVersion::Total))
            } else {
                rewrite.push((term, RelationVersion::Delta))
            }
        }

        rewrites.push(rewrite);
    }

    // Simple query planner, sort terms so that:
    // 1. delta relations are searched first
    // 2. followed by terms with the fewest variables
    for rewrite in rewrites.iter_mut() {
        rewrite.sort_by(|a, b| match (a.1, b.1) {
            (RelationVersion::Delta, RelationVersion::Total) => Ordering::Greater,
            (RelationVersion::Total, RelationVersion::Delta) => Ordering::Less,
            _ => a.0.vars().len().cmp(&b.0.vars().len()),
        })
    }

    rewrites
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

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use anyhow::Result;

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

use super::{
    ast::{
        cid_value::CidValue, declaration::Declaration, fact::Fact, program::Program, rule::Rule,
        stratum::Stratum, GetLink, Negation, RelPredicate, VarPredicate,
    },
    stratify::stratify,
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
            vec![],
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
        let ordered = order_terms(rewrite);

        let operation = lower_rule_body_to_ram(
            rule,
            version,
            Default::default(),
            Default::default(),
            ordered.into_iter().rev().collect(),
            vec![],
            edb,
            idb,
        )?;

        statements.push(Statement::Insert(Insert::new(operation, false)));
    }

    Ok(statements)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_rule_body_to_ram<EF, IF, ER, IR>(
    rule: &Rule,
    version: RelationVersion,
    bindings: im::HashMap<VarId, Term>,
    mut next_alias: im::HashMap<RelationId, AliasId>,
    mut terms: Vec<SemiNaiveTerm>,
    mut formulae: Vec<Formula<EF, IF, ER, IR>>,
    edb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<ER>>>,
    idb: &HashMap<(RelationId, RelationVersion), Arc<RwLock<IR>>>,
) -> Result<ram::Operation<EF, IF, ER, IR>>
where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    match terms.pop() {
        Some(SemiNaiveTerm::RelPredicate(inner, inner_version)) => {
            let mut next_bindings = bindings.clone();
            let alias = next_alias.get(&inner.relation().id()).copied();

            next_alias =
                next_alias.update_with(inner.relation().id(), AliasId::default(), |old, _| {
                    old.next()
                });

            if let Some(cid) = inner.cid() {
                if inner.relation().source() != Source::Edb {
                    return error(Error::ContentAddressedIDB(inner.relation().id()));
                }

                if let CidValue::Var(var) = cid {
                    if !bindings.contains_key(&var.id()) {
                        next_bindings.insert(var.id(), Term::Cid(inner.relation().id(), alias));
                    }
                }
            }

            for (col_id, col_val) in inner.args() {
                if let ColVal::Binding(var) = col_val {
                    if !bindings.contains_key(&var.id()) {
                        next_bindings
                            .insert(var.id(), Term::Col(inner.relation().id(), alias, *col_id));
                    }
                };
            }

            if rule
                .args()
                .clone()
                .into_values()
                .filter_map(|v| match v {
                    ColVal::Binding(v) => Some(v),
                    _ => None,
                })
                .all(|v| next_bindings.contains_key(&v.id()))
            {
                let relation = idb
                    .get(&(rule.head(), RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?
                    .clone();

                let mut cols = im::HashMap::<ColId, Term>::default();
                for (&k, v) in rule.args() {
                    let term = match v {
                        ColVal::Lit(c) => Term::Lit(c.clone()),
                        ColVal::Binding(v) => next_bindings
                            .get(&v.id())
                            .ok_or_else(|| {
                                Error::InternalRhizomeError(format!(
                                    "binding not found: {}",
                                    v.id()
                                ))
                            })?
                            .clone(),
                    };

                    cols.insert(k, term);
                }

                formulae.push(Formula::not_in(
                    rule.head(),
                    RelationVersion::Total,
                    Vec::from_iter(cols.clone()),
                    NotInRelation::Idb(relation),
                ))
            }

            if let Some(cid_val) = inner.cid() {
                match cid_val {
                    CidValue::Cid(cid) => {
                        let formula = Formula::equality(
                            Term::Cid(inner.relation().id(), alias),
                            Term::Lit(Arc::new(Val::Cid(*cid))),
                        );

                        formulae.push(formula);
                    }
                    CidValue::Var(var) => {
                        if let Some(bound) = bindings.get(&var.id()) {
                            let formula = Formula::equality(
                                Term::Cid(inner.relation().id(), alias),
                                bound.clone(),
                            );

                            formulae.push(formula);
                        }
                    }
                }
            }

            let mut rel_bindings = Vec::default();
            for (&col_id, col_val) in inner.args() {
                match col_val {
                    ColVal::Lit(val) => {
                        rel_bindings.push((col_id, Term::Lit(val.clone())));
                    }
                    ColVal::Binding(var) => {
                        if let Some(bound) = bindings.get(&var.id()) {
                            rel_bindings.push((col_id, bound.clone()));
                        }
                    }
                }
            }

            let search_relation = match inner.relation().source() {
                Source::Edb => {
                    let relation = Arc::clone(
                        edb.get(&(inner.relation().id(), inner_version))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    SearchRelation::Edb(relation)
                }
                Source::Idb => {
                    let relation = Arc::clone(
                        idb.get(&(inner.relation().id(), inner_version))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    SearchRelation::Idb(relation)
                }
            };

            Ok(Operation::Search(Search::new(
                inner.relation().id(),
                alias,
                inner_version,
                search_relation,
                rel_bindings,
                formulae,
                lower_rule_body_to_ram(
                    rule,
                    version,
                    next_bindings,
                    next_alias,
                    terms,
                    vec![],
                    edb,
                    idb,
                )?,
            )))
        }
        Some(SemiNaiveTerm::VarPredicate(inner)) => {
            let formula = lower_var_predicate_to_ram(&inner, &bindings)?;

            formulae.push(formula);

            lower_rule_body_to_ram(
                rule, version, bindings, next_alias, terms, formulae, edb, idb,
            )
        }
        Some(SemiNaiveTerm::Negation(inner)) => {
            let formula_delta =
                lower_negation_to_ram(&inner, &bindings, RelationVersion::Delta, edb, idb)?;
            let formula_total =
                lower_negation_to_ram(&inner, &bindings, RelationVersion::Total, edb, idb)?;

            formulae.push(formula_delta);
            formulae.push(formula_total);

            lower_rule_body_to_ram(
                rule, version, bindings, next_alias, terms, formulae, edb, idb,
            )
        }
        Some(SemiNaiveTerm::GetLink(inner)) => {
            let mut next_bindings = bindings.clone();

            if let CidValue::Var(val_var) = inner.link_value() {
                if !bindings.contains_key(&val_var.id()) {
                    match inner.cid() {
                        CidValue::Cid(cid) => {
                            next_bindings.insert(
                                val_var.id(),
                                Term::Link(
                                    inner.link_id(),
                                    Box::new(Term::Lit(Arc::new(Val::Cid(cid)))),
                                ),
                            );
                        }
                        CidValue::Var(var) => {
                            if let Some(term) = bindings.get(&var.id()) {
                                next_bindings.insert(
                                    val_var.id(),
                                    Term::Link(inner.link_id(), Box::new(term.clone())),
                                );
                            } else {
                                return error(Error::ClauseNotDomainIndependent(var.id()));
                            }
                        }
                    };
                }
            }

            let formula = lower_get_link_to_ram(&inner, &next_bindings)?;

            formulae.push(formula);

            lower_rule_body_to_ram(
                rule,
                version,
                next_bindings,
                next_alias,
                terms,
                formulae,
                edb,
                idb,
            )
        }
        Some(SemiNaiveTerm::Reduce(inner)) => {
            let mut next_bindings = bindings.clone();
            let alias = next_alias.get(&inner.relation().id()).copied();

            next_alias =
                next_alias.update_with(inner.relation().id(), AliasId::default(), |old, _| {
                    old.next()
                });

            let mut args = Vec::default();
            let mut group_by_cols = HashMap::default();

            for (col_id, col_val) in inner.group_by_cols() {
                if let Some(term) = match col_val {
                    ColVal::Lit(lit) => Some(Term::Lit(lit.clone())),
                    ColVal::Binding(var) => {
                        if let Some(term) = bindings.get(&var.id()) {
                            if inner.vars().contains(var) {
                                args.push(term.clone());
                            }

                            Some(term.clone())
                        } else if inner.vars().contains(var) {
                            args.push(Term::Col(inner.relation().id(), alias, *col_id));

                            None
                        } else {
                            return error(Error::ClauseNotDomainIndependent(var.id()));
                        }
                    }
                } {
                    group_by_cols.insert(*col_id, term);
                }
            }

            let reduce_relation = match inner.relation().source() {
                Source::Edb => {
                    let relation = Arc::clone(
                        edb.get(&(inner.relation().id(), RelationVersion::Total))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    ReduceRelation::Edb(relation)
                }
                Source::Idb => {
                    let relation = Arc::clone(
                        idb.get(&(inner.relation().id(), RelationVersion::Total))
                            .ok_or_else(|| {
                                Error::InternalRhizomeError("relation not found".to_owned())
                            })?,
                    );

                    ReduceRelation::Idb(relation)
                }
            };

            next_bindings.insert(
                inner.target().id(),
                Term::Agg(inner.relation().id(), alias, *inner.target()),
            );

            if rule
                .args()
                .clone()
                .into_values()
                .filter_map(|v| match v {
                    ColVal::Binding(v) => Some(v),
                    _ => None,
                })
                .all(|v| next_bindings.contains_key(&v.id()))
            {
                let relation = idb
                    .get(&(rule.head(), RelationVersion::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?
                    .clone();

                let mut cols = im::HashMap::<ColId, Term>::default();
                for (&k, v) in rule.args() {
                    let term = match v {
                        ColVal::Lit(c) => Term::Lit(c.clone()),
                        ColVal::Binding(v) => next_bindings
                            .get(&v.id())
                            .ok_or_else(|| {
                                Error::InternalRhizomeError(format!(
                                    "binding not found: {}",
                                    v.id()
                                ))
                            })?
                            .clone(),
                    };

                    cols.insert(k, term);
                }

                formulae.push(Formula::not_in(
                    rule.head(),
                    RelationVersion::Total,
                    Vec::from_iter(cols.clone()),
                    NotInRelation::Idb(relation),
                ));
            }

            Ok(Operation::Reduce(Reduce::new(
                args,
                inner.init().clone(),
                inner.f(),
                *inner.target(),
                group_by_cols,
                inner.relation().id(),
                alias,
                reduce_relation,
                formulae,
                lower_rule_body_to_ram(
                    rule,
                    version,
                    next_bindings,
                    next_alias,
                    terms,
                    vec![],
                    edb,
                    idb,
                )?,
            )))
        }
        None => {
            let relation = idb
                .get(&(rule.head(), version))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?
                .clone();

            let mut cols = im::HashMap::<ColId, Term>::default();
            for (&k, v) in rule.args() {
                let term = match v {
                    ColVal::Lit(c) => Term::Lit(c.clone()),
                    ColVal::Binding(v) => bindings
                        .get(&v.id())
                        .ok_or_else(|| {
                            Error::InternalRhizomeError(format!("binding not found: {}", v.id()))
                        })?
                        .clone(),
                };

                cols.insert(k, term);
            }

            Ok(Operation::Project(Project::new(
                rule.head(),
                version,
                cols,
                formulae,
                relation,
            )))
        }
    }
}

pub(crate) fn lower_negation_to_ram<EF, IF, ER, IR>(
    negation: &Negation,
    bindings: &im::HashMap<VarId, Term>,
    version: RelationVersion,
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
                .ok_or_else(|| {
                    Error::InternalRhizomeError(format!("binding not found: {}", var.id()))
                })?
                .clone(),
        };

        cols.insert(*k, term);
    }

    let not_in_relation = match negation.relation().source() {
        Source::Edb => NotInRelation::Edb(Arc::clone(
            edb.get(&(negation.relation().id(), version))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        )),

        Source::Idb => NotInRelation::Idb(Arc::clone(
            idb.get(&(negation.relation().id(), version))
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
                    .ok_or_else(|| {
                        Error::InternalRhizomeError(format!("binding not found: {}", var.id()))
                    })?
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
            .ok_or_else(|| Error::InternalRhizomeError(format!("binding not found: {}", var.id())))?
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
                .ok_or_else(|| {
                    Error::InternalRhizomeError(format!("binding not found: {}", var.id()))
                })?
                .clone())
        })
        .collect::<Result<Vec<Term>>>()?;

    Ok(Formula::predicate(var_terms, var_predicate.f()))
}

#[derive(Clone, Debug)]
pub(crate) enum SemiNaiveTerm {
    RelPredicate(RelPredicate, RelationVersion),
    VarPredicate(VarPredicate),
    Negation(Negation),
    GetLink(GetLink),
    Reduce(super::ast::body_term::Reduce),
}

pub(crate) fn semi_naive_rewrites(rule: &Rule) -> Vec<Vec<SemiNaiveTerm>> {
    let mut non_relational_terms = vec![];

    for var_predicate in rule.var_predicate_terms() {
        non_relational_terms.push(SemiNaiveTerm::VarPredicate(var_predicate.clone()));
    }

    for negation in rule.negation_terms() {
        non_relational_terms.push(SemiNaiveTerm::Negation(negation.clone()));
    }

    for get_link in rule.get_link_terms() {
        non_relational_terms.push(SemiNaiveTerm::GetLink(get_link.clone()));
    }

    for reduce in rule.reduce_terms() {
        non_relational_terms.push(SemiNaiveTerm::Reduce(reduce.clone()));
    }

    if rule.rel_predicate_terms().is_empty() {
        return vec![non_relational_terms];
    }

    let mut rewrites: Vec<Vec<SemiNaiveTerm>> = vec![];

    // Use a bitmask to represent all of the possible rewrites of the rule,
    // where each rel_predicate searches against either the delta or total
    // relation. The valid semi-naive rewrites will then be the non-zero
    // bitmasks, where a 0 bit corresponds to a search against a total
    // relation, and a 1 against a delta relation.
    let rewrite_count = 1 << rule.rel_predicate_terms().len();

    for offset in 1..rewrite_count {
        let mut rewrite = non_relational_terms.clone();

        for (i, &term) in rule.rel_predicate_terms().iter().enumerate() {
            if offset & (1 << i) == 0 {
                rewrite.push(SemiNaiveTerm::RelPredicate(
                    term.clone(),
                    RelationVersion::Total,
                ))
            } else {
                rewrite.push(SemiNaiveTerm::RelPredicate(
                    term.clone(),
                    RelationVersion::Delta,
                ))
            }
        }

        rewrites.push(rewrite);
    }

    rewrites
}

fn order_terms(mut terms: Vec<SemiNaiveTerm>) -> Vec<SemiNaiveTerm> {
    let mut ordered_terms = Vec::new();
    let mut bindings: HashSet<VarId> = HashSet::new();

    while !terms.is_empty() {
        // Select a term based on the current bindings
        if let Some(term) = select_term(&mut terms, &bindings) {
            // Update the bindings based on the selected term
            update_bindings(&mut bindings, &term);
            // Add the selected term to the ordered list
            ordered_terms.push(term);
        } else {
            panic!();
        }
    }

    ordered_terms
}

fn select_term(
    available_terms: &mut Vec<SemiNaiveTerm>,
    bindings: &HashSet<VarId>,
) -> Option<SemiNaiveTerm> {
    let select_index = available_terms
        .iter()
        .enumerate()
        .filter(|(_, term)| match term {
            SemiNaiveTerm::RelPredicate(_, _) => true,
            SemiNaiveTerm::VarPredicate(inner) => inner.is_vars_bound(bindings),
            SemiNaiveTerm::Negation(inner) => inner.is_vars_bound(bindings),
            SemiNaiveTerm::GetLink(_) => true,
            SemiNaiveTerm::Reduce(_) => true,
        })
        .max_by_key(|(_, term)| match term {
            SemiNaiveTerm::Negation(inner) => (4, inner.vars().len()),
            SemiNaiveTerm::GetLink(inner) => {
                if inner.len_bound_args(bindings) == 2 {
                    (4, 2)
                } else {
                    (0, inner.len_bound_args(bindings))
                }
            }
            SemiNaiveTerm::VarPredicate(inner) => (3, inner.vars().len()),
            SemiNaiveTerm::RelPredicate(inner, RelationVersion::Delta) => {
                (2, inner.bound_vars(bindings).len())
            }
            SemiNaiveTerm::RelPredicate(inner, RelationVersion::Total) => {
                (1, inner.bound_vars(bindings).len())
            }
            SemiNaiveTerm::RelPredicate(_, RelationVersion::New) => {
                panic!("New relation in semi-naive rule");
            }
            SemiNaiveTerm::Reduce(inner) => (0, inner.bound_vars(bindings).len()),
        })
        .map(|(index, _)| index);

    select_index.map(|index| available_terms.remove(index))
}

fn update_bindings(bindings: &mut HashSet<VarId>, term: &SemiNaiveTerm) {
    match term {
        SemiNaiveTerm::GetLink(inner) => {
            if let CidValue::Var(var) = inner.cid() {
                bindings.insert(var.id());
            }
            if let CidValue::Var(var) = inner.link_value() {
                bindings.insert(var.id());
            }
        }
        SemiNaiveTerm::Reduce(inner) => {
            bindings.insert(inner.target().id());
        }
        SemiNaiveTerm::RelPredicate(inner, _) => {
            if let Some(CidValue::Var(var)) = inner.cid() {
                bindings.insert(var.id());
            }

            for var in inner.vars() {
                bindings.insert(var.id());
            }
        }
        _ => {}
    }
}

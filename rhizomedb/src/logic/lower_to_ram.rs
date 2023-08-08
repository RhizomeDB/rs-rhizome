use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use anyhow::Result;

use crate::{
    col_val::ColVal,
    error::{error, Error},
    id::{ColId, RelationId, VarId},
    ram::{
        self, Aggregation, AliasId, ExitBuilder, Formula, Insert, Loop, Merge, Operation, Project,
        Purge, Search, SinksBuilder, SourcesBuilder, Statement, Swap, Term,
    },
    relation::{Relation, RelationKey, Source, Version},
    value::Val,
};

use super::{
    ast::{
        cid_value::CidValue, declaration::Declaration, fact::Fact, program::Program, rule::Rule,
        stratum::Stratum, Negation, RelPredicate, VarPredicate,
    },
    stratify::stratify,
};

pub(crate) fn lower_to_ram(program: &Program) -> Result<ram::program::Program> {
    let mut relations: HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>> = HashMap::default();

    let mut inputs: Vec<&Declaration> = Vec::default();
    let mut outputs: Vec<&Declaration> = Vec::default();
    let mut statements: Vec<Statement> = Vec::default();
    let strata = stratify(program)?;

    for declaration in program.declarations() {
        relations.insert(
            (declaration.id(), Version::New),
            Arc::new(RwLock::new(declaration.relation())),
        );

        relations.insert(
            (declaration.id(), Version::Delta),
            Arc::new(RwLock::new(declaration.relation())),
        );

        relations.insert(
            (declaration.id(), Version::Total),
            Arc::new(RwLock::new(declaration.relation())),
        );

        match declaration.source() {
            Source::Edb => {
                inputs.push(declaration);
            }
            Source::Idb => {
                outputs.push(declaration);
            }
        }
    }

    // Run sources for each input
    if !inputs.is_empty() {
        let mut sources_builder = SourcesBuilder::default();

        for input in &inputs {
            let id = input.id();
            let relation = Arc::clone(
                relations
                    .get(&(id, Version::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            sources_builder.add_relation(id, relation);
        }

        statements.push(Statement::Sources(sources_builder.finalize()));
    }

    for stratum in &strata {
        let mut lowered = lower_stratum_to_ram(stratum, program, &relations)?;

        statements.append(&mut lowered);
    }

    // Merge all newly received input fact into Total and then purge Delta
    for input in &inputs {
        let id = input.id();

        let delta_relation = Arc::clone(
            relations
                .get(&(id, Version::Delta))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        let total_relation = Arc::clone(
            relations
                .get(&(id, Version::Total))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        let merge = Merge::new(
            (id, Version::Delta),
            (id, Version::Total),
            Arc::clone(&delta_relation),
            total_relation,
        );

        statements.push(Statement::Merge(merge));

        statements.push(Statement::Purge(Purge::new(
            (id, Version::Delta),
            delta_relation,
        )));
    }

    // Purge Delta for all outputs
    for output in &outputs {
        let id = output.id();
        let relation = Arc::clone(
            relations
                .get(&(id, Version::Delta))
                .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
        );

        statements.push(Statement::Purge(Purge::new((id, Version::Delta), relation)));
    }

    // TODO: The first strata should never be recursive, but maybe we should assert that
    // during stratification?
    let is_monotonic = strata.len() == 1 && !strata[0].is_recursive();
    let statements = statements.into_iter().map(Arc::new).collect();

    Ok(ram::program::Program::new(
        is_monotonic,
        relations,
        statements,
    ))
}

pub(crate) fn lower_stratum_to_ram(
    stratum: &Stratum<'_>,
    program: &Program,
    relations: &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
) -> Result<Vec<Statement>> {
    let mut statements: Vec<Statement> = Vec::default();

    if stratum.is_recursive() {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(fact, relations)?;

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
            let mut lowered = lower_rule_to_ram(rule, stratum, program, Version::Delta, relations)?;

            statements.append(&mut lowered);
        }

        // Merge the output of the static rules into total
        for relation in HashSet::<RelationId>::from_iter(static_rules.iter().map(|r| r.head())) {
            let from_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                (relation, Version::Delta),
                (relation, Version::Total),
                Arc::clone(&from_relation),
                into_relation,
            );

            statements.push(Statement::Merge(merge));
        }

        let mut loop_body: Vec<Statement> = Vec::default();

        // Purge new, computed during the last loop iteration
        for &id in stratum.relations() {
            let relation = Arc::clone(
                relations
                    .get(&(id, Version::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let statement = Statement::Purge(Purge::new((id, Version::New), relation));

            loop_body.push(statement);
        }

        // Evaluate dynamic rules within the loop, inserting into new
        for rule in &dynamic_rules {
            let mut lowered = lower_rule_to_ram(rule, stratum, program, Version::New, relations)?;

            loop_body.append(&mut lowered);
        }

        // Run sinks for the stratum
        let mut sinks_builder = SinksBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                relations
                    .get(&(id, Version::Delta))
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
                relations
                    .get(&(relation, Version::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                (relation, Version::Delta),
                (relation, Version::Total),
                Arc::clone(&from_relation),
                into_relation,
            );

            loop_body.push(Statement::Merge(merge));
        }

        // Exit the loop if all of the dynamic relations have reached a fixed point
        let mut exit_builder = ExitBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                relations
                    .get(&(id, Version::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            exit_builder.add_relation((id, Version::New), relation);
        }

        loop_body.push(Statement::Exit(exit_builder.finalize()));

        //Swap new and delta
        for &relation in stratum.relations() {
            // Swap new and delta
            let left_relation = Arc::clone(
                relations
                    .get(&(relation, Version::New))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let right_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let swap = Swap::new(
                (relation, Version::New),
                (relation, Version::Delta),
                left_relation,
                right_relation,
            );

            loop_body.push(Statement::Swap(swap));
        }

        let loop_body: Vec<Arc<Statement>> = loop_body.into_iter().map(Arc::new).collect();

        statements.push(Statement::Loop(Loop::new(loop_body)));
    } else {
        // Merge facts into delta
        for fact in stratum.facts() {
            let lowered = lower_fact_to_ram(fact, relations)?;

            statements.push(lowered);
        }

        // Evaluate all rules, inserting into Delta
        for rule in stratum.rules() {
            let mut lowered = lower_rule_to_ram(rule, stratum, program, Version::Delta, relations)?;

            statements.append(&mut lowered);
        }

        // Merge rules from Delta into Total
        for &relation in stratum.relations() {
            let from_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Delta))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let into_relation = Arc::clone(
                relations
                    .get(&(relation, Version::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            let merge = Merge::new(
                (relation, Version::Delta),
                (relation, Version::Total),
                Arc::clone(&from_relation),
                into_relation,
            );

            statements.push(Statement::Merge(merge));
        }

        // Run sinks for the stratum
        let mut sinks_builder = SinksBuilder::default();

        for &id in stratum.relations() {
            let relation = Arc::clone(
                relations
                    .get(&(id, Version::Delta))
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

pub(crate) fn lower_fact_to_ram(
    fact: &Fact,
    relations: &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
) -> Result<Statement> {
    let cols = fact.args().iter().map(|(k, v)| (*k, Term::Lit(v.clone())));

    let relation = Arc::clone(
        relations
            .get(&(fact.head(), Version::Delta))
            .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
    );

    Ok(Statement::Insert(Insert::new(
        Operation::Project(Project::new(
            (fact.head(), Version::Delta),
            cols,
            vec![],
            relation,
        )),
        true,
    )))
}

pub(crate) fn lower_rule_to_ram(
    rule: &Rule,
    _stratum: &Stratum<'_>,
    _program: &Program,
    version: Version,
    relations: &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
) -> Result<Vec<Statement>> {
    let mut statements: Vec<Statement> = Vec::default();

    for rewrite in semi_naive_rewrites(rule) {
        let ordered = order_terms(rewrite);

        let operation = lower_rule_body_to_ram(
            rule,
            version,
            Default::default(),
            Default::default(),
            ordered.into_iter().rev().collect(),
            vec![],
            relations,
        )?;

        statements.push(Statement::Insert(Insert::new(operation, false)));
    }

    Ok(statements)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_rule_body_to_ram(
    rule: &Rule,
    version: Version,
    bindings: im::HashMap<VarId, Term>,
    mut next_alias: im::HashMap<RelationId, AliasId>,
    mut terms: Vec<SemiNaiveTerm>,
    mut formulae: Vec<Formula>,
    relations: &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
) -> Result<ram::Operation> {
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
                let relation = relations
                    .get(&(rule.head(), Version::Total))
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
                    Version::Total,
                    Vec::from_iter(cols.clone()),
                    relation,
                ))
            }

            if let Some(cid_val) = inner.cid() {
                match cid_val {
                    CidValue::Cid(cid) => {
                        let formula = Formula::equality(
                            Term::Cid(inner.relation().id(), alias),
                            Term::Lit(Val::Cid(*cid)),
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

            let search_relation = Arc::clone(
                relations
                    .get(&(inner.relation().id(), inner_version))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

            Ok(Operation::Search(Search::new(
                (inner.relation().id(), inner_version),
                alias,
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
                    relations,
                )?,
            )))
        }
        Some(SemiNaiveTerm::VarPredicate(inner)) => {
            let formula = lower_var_predicate_to_ram(&inner, &bindings)?;

            formulae.push(formula);

            lower_rule_body_to_ram(
                rule, version, bindings, next_alias, terms, formulae, relations,
            )
        }
        Some(SemiNaiveTerm::Negation(inner)) => {
            let formula_delta =
                lower_negation_to_ram(&inner, &bindings, Version::Delta, relations)?;
            let formula_total =
                lower_negation_to_ram(&inner, &bindings, Version::Total, relations)?;

            formulae.push(formula_delta);
            formulae.push(formula_total);

            lower_rule_body_to_ram(
                rule, version, bindings, next_alias, terms, formulae, relations,
            )
        }
        Some(SemiNaiveTerm::Aggregation(inner)) => {
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
                            None
                        }
                    }
                } {
                    group_by_cols.insert(*col_id, term);
                }
            }

            let aggregation_relation = Arc::clone(
                relations
                    .get(&(inner.relation().id(), Version::Total))
                    .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?,
            );

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
                let relation = relations
                    .get(&(rule.head(), Version::Total))
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
                    Version::Total,
                    Vec::from_iter(cols.clone()),
                    relation,
                ));
            }

            Ok(Operation::Aggregation(Aggregation::new(
                args,
                inner.agg(),
                *inner.target(),
                group_by_cols,
                inner.relation().id(),
                alias,
                aggregation_relation,
                formulae,
                lower_rule_body_to_ram(
                    rule,
                    version,
                    next_bindings,
                    next_alias,
                    terms,
                    vec![],
                    relations,
                )?,
            )))
        }
        None => {
            let relation = relations
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
                (rule.head(), version),
                cols,
                formulae,
                relation,
            )))
        }
    }
}

pub(crate) fn lower_negation_to_ram(
    negation: &Negation,
    bindings: &im::HashMap<VarId, Term>,
    version: Version,
    relations: &HashMap<RelationKey, Arc<RwLock<Box<dyn Relation>>>>,
) -> Result<Formula> {
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

    let not_in_relation = relations
        .get(&(negation.relation().id(), version))
        .ok_or_else(|| Error::InternalRhizomeError("relation not found".to_owned()))?;

    Ok(Formula::not_in(
        negation.relation().id(),
        version,
        cols,
        Arc::clone(not_in_relation),
    ))
}

pub(crate) fn lower_var_predicate_to_ram(
    var_predicate: &VarPredicate,
    bindings: &im::HashMap<VarId, Term>,
) -> Result<Formula> {
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

#[derive(Debug, Clone)]
pub(crate) enum SemiNaiveTerm {
    RelPredicate(RelPredicate, Version),
    VarPredicate(VarPredicate),
    Negation(Negation),
    Aggregation(super::ast::body_term::Aggregation),
}

pub(crate) fn semi_naive_rewrites(rule: &Rule) -> Vec<Vec<SemiNaiveTerm>> {
    let mut non_relational_terms = vec![];

    for var_predicate in rule.var_predicate_terms() {
        non_relational_terms.push(SemiNaiveTerm::VarPredicate(var_predicate.clone()));
    }

    for negation in rule.negation_terms() {
        non_relational_terms.push(SemiNaiveTerm::Negation(negation.clone()));
    }

    for aggregation in rule.aggregation_terms() {
        non_relational_terms.push(SemiNaiveTerm::Aggregation(aggregation.clone()));
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
                rewrite.push(SemiNaiveTerm::RelPredicate(term.clone(), Version::Total))
            } else {
                rewrite.push(SemiNaiveTerm::RelPredicate(term.clone(), Version::Delta))
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
            SemiNaiveTerm::Aggregation(_) => true,
        })
        .max_by_key(|(_, term)| match term {
            SemiNaiveTerm::Negation(inner) => (4, inner.vars().len()),
            SemiNaiveTerm::VarPredicate(inner) => (3, inner.vars().len()),
            SemiNaiveTerm::RelPredicate(inner, Version::Delta) => {
                (2, inner.bound_vars(bindings).len())
            }
            SemiNaiveTerm::RelPredicate(inner, Version::Total) => {
                (1, inner.bound_vars(bindings).len())
            }
            SemiNaiveTerm::RelPredicate(_, Version::New) => {
                panic!("New relation in semi-naive rule");
            }
            SemiNaiveTerm::Aggregation(inner) => (0, inner.bound_vars(bindings).len()),
        })
        .map(|(index, _)| index);

    select_index.map(|index| available_terms.remove(index))
}

fn update_bindings(bindings: &mut HashSet<VarId>, term: &SemiNaiveTerm) {
    match term {
        SemiNaiveTerm::Aggregation(inner) => {
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

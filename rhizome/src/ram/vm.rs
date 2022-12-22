use std::collections::{BTreeMap, HashSet};

use crate::{datum::Datum, fact::Fact, id::AttributeId, timestamp::Timestamp};

use super::ast::*;

#[derive(Clone, Debug)]
pub struct VM<T: Timestamp> {
    timestamp: T,
    pc: (usize, Option<usize>),
    program: Program,
    // TODO: Better data structure
    relations: BTreeMap<Relation, HashSet<Fact<T>>>,
}

impl<T: Timestamp> VM<T> {
    pub fn new(program: Program) -> Self {
        Self {
            timestamp: T::default(),
            pc: (0, None),
            program,
            relations: BTreeMap::<Relation, HashSet<Fact<T>>>::default(),
        }
    }

    pub fn step(&mut self) {
        match self.load_statement().clone() {
            Statement::Insert { operation } => self.handle_operation(operation),
            Statement::Merge { from, into } => self.handle_merge(from, into),
            Statement::Swap { left, right } => self.handle_swap(left, right),
            Statement::Purge { relation } => self.handle_purge(relation),
            Statement::Exit { relations } => {
                assert!(self.pc.1.is_some());

                self.handle_exit(relations);
            }
            Statement::Loop { .. } => {
                unreachable!("load_statement follows loops in the root block")
            }
        };

        self.pc = self.step_pc();

        if self.pc.0 == 0 {
            self.timestamp = self.timestamp.advance_epoch();
        } else if self.pc.1 == Some(0) {
            self.timestamp = self.timestamp.advance_iteration();
        };
    }

    fn step_pc(&self) -> (usize, Option<usize>) {
        match self.pc {
            (outer, None) => {
                if let Some(Statement::Loop { .. }) = self.program.statements().get(self.pc.0 + 1) {
                    ((outer + 1) % self.program.statements().len(), Some(0))
                } else {
                    ((outer + 1) % self.program.statements().len(), None)
                }
            }
            (outer, Some(inner)) => {
                if let Some(Statement::Loop { body }) = self.program.statements().get(self.pc.0) {
                    (outer, Some((inner + 1) % body.len()))
                } else {
                    unreachable!("Current statement must be a loop!")
                }
            }
        }
    }

    fn load_statement(&self) -> &Statement {
        match self.program.statements().get(self.pc.0).unwrap() {
            Statement::Loop { body } => {
                assert!(self.pc.1.is_some());

                body.get(self.pc.1.unwrap()).unwrap()
            }
            s => {
                assert!(self.pc.1.is_none());

                s
            }
        }
    }

    fn handle_operation(&mut self, operation: Operation) {
        let bindings: BTreeMap<RelationBinding, Fact<T>> = BTreeMap::default();

        self.do_handle_operation(operation, bindings);
    }

    fn do_handle_operation(
        &mut self,
        operation: Operation,
        bindings: BTreeMap<RelationBinding, Fact<T>>,
    ) {
        match operation {
            Operation::Search {
                relation,
                alias,
                when,
                operation,
            } => {
                let relation_binding = RelationBinding::new(*relation.id(), alias);
                let facts = self.relations.entry(relation).or_default().clone();

                for fact in facts.iter() {
                    let mut next_bindings = bindings.clone();

                    let is_satisfied = when.iter().all(|f| match f {
                        Formula::Equality(equality) => {
                            // TODO: Dry up dereferencing Term -> Datum
                            let left_value = match &equality.left() {
                                Term::Attribute(attribute)
                                    if *attribute.relation() == relation_binding =>
                                {
                                    fact.attribute(attribute.id()).unwrap()
                                }
                                Term::Attribute(attribute) => next_bindings
                                    .get(attribute.relation())
                                    .unwrap()
                                    .attribute(attribute.id())
                                    .unwrap(),
                                Term::Literal(literal) => literal.datum(),
                            };

                            let right_value = match &equality.right() {
                                Term::Attribute(attribute)
                                    if *attribute.relation() == relation_binding =>
                                {
                                    fact.attribute(attribute.id()).unwrap()
                                }
                                Term::Attribute(attribute) => next_bindings
                                    .get(attribute.relation())
                                    .unwrap()
                                    .attribute(attribute.id())
                                    .unwrap(),
                                Term::Literal(literal) => literal.datum(),
                            };

                            left_value == right_value
                        }
                        Formula::NotIn(not_in) => {
                            // TODO: Dry up constructing a fact from BTreeMap<AttributeId, Term>
                            let mut bound: Vec<(AttributeId, Datum)> = Vec::default();

                            for (id, term) in not_in.attributes() {
                                match term {
                                    Term::Attribute(attribute) => {
                                        let fact = next_bindings.get(attribute.relation()).unwrap();

                                        bound.push((*id, *fact.attribute(attribute.id()).unwrap()));
                                    }
                                    Term::Literal(literal) => bound.push((*id, *literal.datum())),
                                }
                            }

                            let bound_fact =
                                Fact::new(*not_in.relation().id(), self.timestamp.clone(), bound);

                            !self
                                .relations
                                .entry(not_in.relation().clone())
                                .or_default()
                                .contains(&bound_fact)
                        }
                    });

                    if !is_satisfied {
                        continue;
                    }

                    next_bindings.insert(relation_binding.clone(), fact.clone());

                    self.do_handle_operation(*operation.clone(), next_bindings.clone());
                }
            }
            Operation::Project { attributes, into } => {
                let mut bound: Vec<(AttributeId, Datum)> = Vec::default();

                for (id, term) in &attributes {
                    match term {
                        Term::Attribute(attribute) => {
                            let fact = bindings.get(attribute.relation()).unwrap();

                            bound.push((*id, *fact.attribute(attribute.id()).unwrap()));
                        }
                        Term::Literal(literal) => bound.push((*id, *literal.datum())),
                    }
                }

                let fact = Fact::new(*into.id(), self.timestamp.clone(), bound);

                self.relations.entry(into).or_default().insert(fact);
            }
        };
    }

    fn handle_merge(&mut self, from: Relation, into: Relation) {
        let from_relation = self.relations.entry(from).or_default().clone();

        self.relations
            .entry(into)
            .and_modify(|r| {
                *r = r.union(&from_relation).cloned().collect();
            })
            .or_insert_with(|| from_relation.clone());
    }

    fn handle_swap(&mut self, left: Relation, right: Relation) {
        let left_relation = self.relations.entry(left.clone()).or_default().clone();
        let right_relation = self.relations.entry(right.clone()).or_default().clone();

        self.relations
            .entry(left)
            .and_modify(|r| *r = right_relation);

        self.relations
            .entry(right)
            .and_modify(|r| *r = left_relation);
    }

    fn handle_purge(&mut self, relation: Relation) {
        self.relations.entry(relation).and_modify(|r| {
            *r = HashSet::default();
        });
    }

    fn handle_exit(&mut self, relations: Vec<Relation>) {
        let is_done = relations
            .iter()
            .all(|r| self.relations.entry(r.clone()).or_default().is_empty());

        if is_done {
            self.pc.1 = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{
        logic::{lower_to_ram, parser},
        timestamp::PairTimestamp,
    };

    use super::*;

    #[test]
    fn test_step() {
        let program = parser::parse(
            r#"
        edge(from: 0, to: 1).
        edge(from: 1, to: 2).
        edge(from: 2, to: 3).
        edge(from: 3, to: 4).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )
        .unwrap();

        let ast = lower_to_ram::lower_to_ram(&program).unwrap();
        let mut vm: VM<PairTimestamp> = VM::new(ast);

        for _ in 0..20 {
            vm.step();
        }

        // TODO: Test specific derived tuples once I actually implement
        // relations using a data structure that supports easier inspection
        assert_eq!(
            10,
            vm.relations
                .clone()
                .get(&Relation::new("path".into(), RelationVersion::Total))
                .unwrap()
                .len()
        );
    }
}

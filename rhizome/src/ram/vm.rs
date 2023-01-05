use im::HashMap;

use crate::{
    datum::Datum,
    fact::Fact,
    id::AttributeId,
    relation::{ImmutableHashSetRelation, Relation},
    timestamp::{PairTimestamp, Timestamp},
};

use super::ast::{Exit, Loop, Merge, Purge, Swap, *};

#[derive(Clone, Debug)]
pub struct VM<T: Timestamp = PairTimestamp, R: Relation<T> = ImmutableHashSetRelation<T>> {
    timestamp: T,
    pc: (usize, Option<usize>),
    program: Program,
    // TODO: Better data structure
    relations: HashMap<RelationRef, R>,
}

impl<T: Timestamp, R: Relation<T>> VM<T, R> {
    pub fn new(program: Program) -> Self {
        Self {
            timestamp: T::default(),
            pc: (0, None),
            program,
            relations: HashMap::<RelationRef, R>::default(),
        }
    }

    pub fn relation(&self, id: &str) -> R {
        self.relations
            .get(&RelationRef::new(id.into(), RelationVersion::Total))
            .cloned()
            .unwrap_or_default()
    }

    pub fn step_epoch(&mut self) {
        let start = self.timestamp;

        loop {
            self.step();

            if self.timestamp.epoch() != start.epoch() {
                break;
            }
        }
    }

    pub fn step(&mut self) {
        match self.load_statement().clone() {
            Statement::Insert(insert) => self.handle_operation(insert.operation()),
            Statement::Merge(merge) => self.handle_merge(&merge),
            Statement::Swap(swap) => self.handle_swap(&swap),
            Statement::Purge(purge) => self.handle_purge(&purge),
            Statement::Exit(exit) => {
                assert!(self.pc.1.is_some());

                self.handle_exit(&exit);
            }
            Statement::Loop(Loop { .. }) => {
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
                if let Some(Statement::Loop(Loop { .. })) =
                    self.program.statements().get(self.pc.0 + 1)
                {
                    ((outer + 1) % self.program.statements().len(), Some(0))
                } else {
                    ((outer + 1) % self.program.statements().len(), None)
                }
            }
            (outer, Some(inner)) => {
                if let Some(Statement::Loop(loop_statement)) =
                    self.program.statements().get(self.pc.0)
                {
                    (outer, Some((inner + 1) % loop_statement.body().len()))
                } else {
                    unreachable!("Current statement must be a loop!")
                }
            }
        }
    }

    fn load_statement(&self) -> &Statement {
        match self.program.statements().get(self.pc.0).unwrap() {
            Statement::Loop(loop_statement) => {
                assert!(self.pc.1.is_some());

                loop_statement.body().get(self.pc.1.unwrap()).unwrap()
            }
            s => {
                assert!(self.pc.1.is_none());

                s
            }
        }
    }

    fn handle_operation(&mut self, operation: &Operation) {
        let bindings = HashMap::<RelationBinding, Fact<T>>::default();

        self.do_handle_operation(operation, bindings)
    }

    fn do_handle_operation(
        &mut self,
        operation: &Operation,
        bindings: HashMap<RelationBinding, Fact<T>>,
    ) {
        match operation {
            Operation::Search(inner) => self.handle_search(inner, bindings),
            Operation::Project(inner) => self.handle_project(inner, bindings),
        }
    }

    fn handle_search(&mut self, search: &Search, bindings: HashMap<RelationBinding, Fact<T>>) {
        let relation_binding = RelationBinding::new(*search.relation().id(), *search.alias());
        let facts = self
            .relations
            .get(search.relation())
            .cloned()
            .unwrap_or_default();

        for fact in facts {
            let mut next_bindings = bindings.clone();

            let is_satisfied = search.when().iter().all(|f| match f {
                Formula::Equality(equality) => {
                    // TODO: Dry up dereferencing Term -> Datum
                    let left_value = match &equality.left() {
                        Term::Attribute(attribute) if *attribute.relation() == relation_binding => {
                            fact.attribute(attribute.id()).unwrap()
                        }
                        Term::Attribute(attribute) => next_bindings
                            .get(attribute.relation())
                            .map(|f| f.attribute(attribute.id()).unwrap())
                            .unwrap(),
                        Term::Literal(literal) => literal.datum(),
                    };

                    let right_value = match &equality.right() {
                        Term::Attribute(attribute) if *attribute.relation() == relation_binding => {
                            fact.attribute(attribute.id()).unwrap()
                        }
                        Term::Attribute(attribute) => next_bindings
                            .get(attribute.relation())
                            .map(|f| f.attribute(attribute.id()).unwrap())
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

                    let bound_fact = Fact::new(*not_in.relation().id(), self.timestamp, bound);

                    !self
                        .relations
                        .get(not_in.relation())
                        .map(|r| r.contains(&bound_fact))
                        .unwrap_or(false)
                }
            });

            if !is_satisfied {
                continue;
            }

            next_bindings.insert(relation_binding, fact.clone());

            self.do_handle_operation(search.operation(), next_bindings);
        }
    }

    fn handle_project(&mut self, project: &Project, bindings: HashMap<RelationBinding, Fact<T>>) {
        let mut bound: Vec<(AttributeId, Datum)> = Vec::default();

        for (id, term) in project.attributes() {
            match term {
                Term::Attribute(attribute) => {
                    let fact = bindings.get(attribute.relation()).unwrap();

                    bound.push((*id, *fact.attribute(attribute.id()).unwrap()));
                }
                Term::Literal(literal) => bound.push((*id, *literal.datum())),
            }
        }

        let fact = Fact::new(*project.into().id(), self.timestamp, bound);

        self.relations = self.relations.alter(
            |old| match old {
                Some(facts) => Some(facts.insert(fact)),
                None => Some(R::from_iter([fact])),
            },
            *project.into(),
        );
    }

    fn handle_merge(&mut self, merge: &Merge) {
        let from_relation = self
            .relations
            .get(merge.from())
            .cloned()
            .unwrap_or_default();

        self.relations = self
            .relations
            .update_with(*merge.into(), from_relation, |old, new| old.merge(new));
    }

    fn handle_swap(&mut self, swap: &Swap) {
        let left_relation = self.relations.remove(swap.left()).unwrap_or_default();
        let right_relation = self.relations.remove(swap.right()).unwrap_or_default();

        self.relations.insert(*swap.left(), right_relation);
        self.relations.insert(*swap.right(), left_relation);
    }

    fn handle_purge(&mut self, purge: &Purge) {
        self.relations = self.relations.update(*purge.relation(), R::default());
    }

    fn handle_exit(&mut self, exit: &Exit) {
        let is_done = exit
            .relations()
            .iter()
            .all(|r| self.relations.get(r).map_or(false, R::is_empty));

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
        relation::ImmutableHashSetRelation,
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
        let mut vm: VM = VM::new(ast);

        vm.step_epoch();

        assert_eq!(
            vm.relation("path"),
            ImmutableHashSetRelation::from_iter([
                Fact::new(
                    "path".into(),
                    (0, 0,).into(),
                    vec![("from".into(), 1.into()), ("to".into(), 2.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 1,).into(),
                    vec![("from".into(), 1.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 0,).into(),
                    vec![("from".into(), 3.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 0,).into(),
                    vec![("from".into(), 2.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 2,).into(),
                    vec![("from".into(), 0.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 3,).into(),
                    vec![("from".into(), 0.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 1,).into(),
                    vec![("from".into(), 2.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 1,).into(),
                    vec![("from".into(), 0.into()), ("to".into(), 2.into())],
                ),
                Fact::new(
                    "path".into(),
                    (0, 0,).into(),
                    vec![("from".into(), 0.into()), ("to".into(), 1.into()),],
                ),
                Fact::new(
                    "path".into(),
                    (0, 2,).into(),
                    vec![("from".into(), 1.into()), ("to".into(), 4.into()),],
                ),
            ])
        );
    }
}

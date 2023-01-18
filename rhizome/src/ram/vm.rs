use core::fmt::Debug;
use std::pin::Pin;

use anyhow::Result;
use futures::{
    executor::{block_on, block_on_stream},
    prelude::*,
    stream::{select_all::SelectAll, AbortHandle, Abortable},
};
use im::HashMap as ImmHashMap;
use slotmap::{new_key_type, SlotMap};

use crate::{
    datum::Datum,
    error::Error,
    fact::Fact,
    id::AttributeId,
    relation::{DefaultRelation, Relation},
    timestamp::{DefaultTimestamp, Timestamp},
};

use super::ast::{Exit, Loop, Merge, Purge, Swap, *};

new_key_type! { pub struct SinkKey; }

pub type FactStream = Pin<Box<dyn Stream<Item = Fact>>>;
pub type FactSink = Pin<Box<dyn Sink<Fact, Error = Error>>>;

pub struct VM<T: Timestamp = DefaultTimestamp, R: Relation = DefaultRelation> {
    timestamp: T,
    pc: (usize, Option<usize>),
    sources: SelectAll<Abortable<FactStream>>,
    // TODO: Find some way of using Abortable with Sink
    sinks: SlotMap<SinkKey, (RelationRef, FactSink)>,
    program: Program,
    // TODO: Better data structure
    relations: ImmHashMap<RelationRef, R>,
}

impl<T: Timestamp, R: Relation> Debug for VM<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VM")
            .field("timestamp", &self.timestamp)
            .field("pc", &self.pc)
            .finish()
    }
}

impl<T: Timestamp, R: Relation> VM<T, R> {
    pub fn new(program: Program) -> Self {
        Self {
            timestamp: T::default(),
            pc: (0, None),
            sources: SelectAll::default(),
            sinks: SlotMap::with_key(),
            program,
            relations: ImmHashMap::<RelationRef, R>::default(),
        }
    }

    pub fn timestamp(&self) -> &T {
        &self.timestamp
    }

    pub fn relation(&self, id: &str) -> R {
        self.relations
            .get(&RelationRef::new(id.into(), RelationVersion::Total))
            .cloned()
            .unwrap_or_default()
    }

    pub fn register_source(&mut self, source: Pin<Box<dyn Stream<Item = Fact>>>) -> AbortHandle {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();

        self.sources
            .push(Abortable::new(source, abort_registration));

        abort_handle
    }

    pub fn register_sink(
        &mut self,
        relation_ref: RelationRef,
        sink: Pin<Box<dyn Sink<Fact, Error = Error>>>,
    ) -> SinkKey {
        self.sinks.insert((relation_ref, sink))
    }

    pub fn unregister_sink(&mut self, sink_key: SinkKey) {
        self.sinks.remove(sink_key);
    }

    pub fn step_epoch(&mut self) -> Result<()> {
        let start = self.timestamp;

        loop {
            self.step()?;

            if self.timestamp.epoch() != start.epoch() {
                break;
            }
        }

        Ok(())
    }

    pub fn step(&mut self) -> Result<()> {
        match self.load_statement().clone() {
            Statement::Insert(insert) => self.handle_operation(insert.operation()),
            Statement::Merge(merge) => self.handle_merge(&merge),
            Statement::Swap(swap) => self.handle_swap(&swap),
            Statement::Purge(purge) => self.handle_purge(&purge),
            Statement::Exit(exit) => {
                assert!(self.pc.1.is_some());

                self.handle_exit(&exit);
            }
            Statement::Sources(sources) => self.handle_sources(&sources)?,
            Statement::Sinks(sinks) => self.handle_sinks(&sinks)?,
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

        Ok(())
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
        let bindings = ImmHashMap::<RelationBinding, Fact>::default();

        self.do_handle_operation(operation, bindings)
    }

    fn do_handle_operation(
        &mut self,
        operation: &Operation,
        bindings: ImmHashMap<RelationBinding, Fact>,
    ) {
        match operation {
            Operation::Search(inner) => self.handle_search(inner, bindings),
            Operation::Project(inner) => self.handle_project(inner, bindings),
        }
    }

    fn handle_search(&mut self, search: &Search, bindings: ImmHashMap<RelationBinding, Fact>) {
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

                    let bound_fact = Fact::new(*not_in.relation().id(), bound);

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

    fn handle_project(&mut self, project: &Project, bindings: ImmHashMap<RelationBinding, Fact>) {
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

        let fact = Fact::new(*project.into().id(), bound);

        self.relations = self.relations.alter(
            |old| match old {
                Some(facts) => Some(facts.insert(fact)),
                None => Some(R::default().insert(fact)),
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

    fn handle_sources(&mut self, _sources: &Sources) -> Result<()> {
        // TODO: should not block here
        let iter = block_on_stream(&mut self.sources);

        for fact in iter {
            let relation_ref = RelationRef::new(*fact.id(), RelationVersion::Total);

            self.relations = self.relations.alter(
                |old| match old {
                    Some(facts) => Some(facts.insert(fact)),
                    None => Some(R::default().insert(fact)),
                },
                relation_ref,
            );
        }

        Ok(())
    }

    fn handle_sinks(&mut self, sinks: &Sinks) -> Result<()> {
        for (_key, (relation_ref, sink)) in &mut self.sinks {
            // TODO: Slow. Index sinks by relation_ref
            if !sinks.relations().contains(relation_ref) {
                continue;
            }

            for fact in self.relations.get(relation_ref).unwrap().clone() {
                // TODO: should not block here
                block_on(sink.send(fact))?;
            }

            // TODO: should not block here
            block_on(sink.flush())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use futures::{sink::unfold, stream};
    use pretty_assertions::assert_eq;

    use crate::logic::{lower_to_ram, parser};

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() {
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

        vm.step_epoch().unwrap();

        assert_eq!(
            vm.relation("path"),
            DefaultRelation::from_iter([
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 2.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 3.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 2.into())],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 1.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 4.into()),],
                ),
            ])
        );
    }

    #[test]
    fn test_source_transitive_closure() {
        let program = parser::parse(
            r#"
        input edge(from, to).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )
        .unwrap();

        let ast = lower_to_ram::lower_to_ram(&program).unwrap();
        let mut vm: VM = VM::new(ast);

        vm.register_source(Box::pin(stream::iter(vec![
            Fact::new(
                "edge".into(),
                vec![("from".into(), 0.into()), ("to".into(), 1.into())],
            ),
            Fact::new(
                "edge".into(),
                vec![("from".into(), 1.into()), ("to".into(), 2.into())],
            ),
            Fact::new(
                "edge".into(),
                vec![("from".into(), 2.into()), ("to".into(), 3.into())],
            ),
            Fact::new(
                "edge".into(),
                vec![("from".into(), 3.into()), ("to".into(), 4.into())],
            ),
        ])));

        vm.step_epoch().unwrap();

        assert_eq!(
            vm.relation("path"),
            DefaultRelation::from_iter([
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 2.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 3.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 2.into())],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 1.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 4.into()),],
                ),
            ])
        );
    }

    #[test]
    fn test_sink_transitive_closure() {
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

        let buf1 = Rc::new(RefCell::new(Vec::<Fact>::new()));
        let buf2 = Rc::clone(&buf1);
        let sink = unfold((), move |_, fact: Fact| {
            let b = Rc::clone(&buf1);
            async move {
                Rc::clone(&b).borrow_mut().push(fact);
                Ok(())
            }
        });

        vm.register_sink(
            RelationRef::new("path".into(), RelationVersion::Total),
            Box::pin(sink),
        );

        vm.step_epoch().unwrap();

        assert_eq!(
            *buf2.borrow(),
            vec![
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 1.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 2.into())],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 0.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 2.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 1.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 3.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 2.into()), ("to".into(), 4.into()),],
                ),
                Fact::new(
                    "path".into(),
                    vec![("from".into(), 3.into()), ("to".into(), 4.into()),],
                ),
            ]
        );
    }
}

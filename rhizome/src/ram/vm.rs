use core::fmt::Debug;

use anyhow::Result;
use futures::channel::mpsc;
use im::HashMap as ImmHashMap;

use crate::{
    datum::Datum,
    fact::Fact,
    id::{AttributeId, RelationId},
    relation::{DefaultRelation, Relation},
    timestamp::{DefaultTimestamp, Timestamp},
};

use super::ast::{Exit, Loop, Merge, Purge, Swap, *};

pub struct VM<T: Timestamp = DefaultTimestamp, R: Relation = DefaultRelation> {
    timestamp: T,
    pc: (usize, Option<usize>),
    input_channels: Vec<mpsc::UnboundedReceiver<Fact>>,
    output_channels: Vec<(RelationId, mpsc::UnboundedSender<Fact>)>,
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
            input_channels: Vec::default(),
            output_channels: Vec::default(),
            program,
            relations: ImmHashMap::<RelationRef, R>::default(),
        }
    }

    pub fn timestamp(&self) -> &T {
        &self.timestamp
    }

    pub fn relation(&self, id: &str) -> R {
        self.relations
            .get(&RelationRef::new(id, RelationVersion::Total))
            .cloned()
            .unwrap_or_default()
    }

    pub fn input_channel(&mut self) -> mpsc::UnboundedSender<Fact> {
        let (tx, rx) = mpsc::unbounded();

        self.input_channels.push(rx);

        tx
    }

    pub fn output_channel(
        &mut self,
        relation_id: impl Into<RelationId>,
    ) -> mpsc::UnboundedReceiver<Fact> {
        let (tx, rx) = mpsc::unbounded();

        self.output_channels.push((relation_id.into(), tx));

        rx
    }

    pub fn step_epoch(&mut self) -> Result<()> {
        assert!(self.timestamp == self.timestamp.epoch_start());

        let mut has_new_facts = false;

        // For each channel, read incoming facts into the correct relation
        self.input_channels.retain_mut(|rx| {
            let mut is_open = true;
            loop {
                match rx.try_next() {
                    Ok(Some(fact)) => {
                        let relation_ref = RelationRef::new(*fact.id(), RelationVersion::Delta);

                        has_new_facts = true;

                        self.relations = self.relations.alter(
                            |old| match old {
                                Some(facts) => Some(facts.insert(fact)),
                                None => Some(R::default().insert(fact)),
                            },
                            relation_ref,
                        );
                    }
                    Ok(None) => {
                        is_open = false;
                        break;
                    }
                    Err(_) => break,
                }
            }

            is_open
        });

        let start = self.timestamp;

        // Only run the epoch if there's new input facts
        // TODO: We also run once at the start, to handle hardcoded facts. This isn't
        // super elegant though.
        if has_new_facts || start == self.timestamp().clock_start() {
            loop {
                self.step()?;

                if self.timestamp.epoch() != start.epoch() {
                    break;
                }
            }
        }

        Ok(())
    }

    fn step(&mut self) -> Result<()> {
        match self.load_statement().clone() {
            Statement::Insert(insert) => self.handle_insert(&insert),
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

    fn handle_insert(&mut self, insert: &Insert) {
        // Only insert ground facts on the first clock cycle
        if insert.is_ground() && *self.timestamp() != self.timestamp().clock_start() {
        } else {
            self.handle_operation(insert.operation());
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

            next_bindings.insert(relation_binding, fact.clone());

            let is_satisfied = search.when().iter().all(|f| match f {
                Formula::Equality(equality) => {
                    let left = equality.left().resolve(&fact, &relation_binding, &bindings);
                    let right = equality
                        .right()
                        .resolve(&fact, &relation_binding, &bindings);

                    left == right
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
                        .unwrap_or_default()
                }
            });

            if !is_satisfied {
                continue;
            }

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

        self.relations = self.relations.update(*swap.left(), right_relation);
        self.relations = self.relations.update(*swap.right(), left_relation);
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
        // TODO: Channels are now read at the start of the epoch; maybe remove this?

        Ok(())
    }

    fn handle_sinks(&mut self, sinks: &Sinks) -> Result<()> {
        self.output_channels.retain(|(relation_id, tx)| {
            let relation_ref = RelationRef::new(*relation_id, RelationVersion::Delta);

            if !sinks.relations().contains(&relation_ref) {
                return true;
            }

            if tx.is_closed() {
                return false;
            }

            if let Some(relation) = self.relations.get(&relation_ref) {
                for fact in relation.clone() {
                    tx.unbounded_send(fact).unwrap();
                }
            }

            !tx.is_closed()
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::{lower_to_ram, parser};
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        edge(from: 0, to: 1).
        edge(from: 1, to: 2).
        edge(from: 2, to: 3).
        edge(from: 3, to: 4).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let mut vm: VM = VM::new(ast);
        let mut rx = vm.output_channel("path");

        vm.step_epoch()?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = rx.try_next() {
            path.insert(fact);
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                Fact::new("path", [("from", 0), ("to", 1)]),
                Fact::new("path", [("from", 0), ("to", 2)]),
                Fact::new("path", [("from", 0), ("to", 3)]),
                Fact::new("path", [("from", 0), ("to", 4)]),
                Fact::new("path", [("from", 1), ("to", 2)]),
                Fact::new("path", [("from", 1), ("to", 3)]),
                Fact::new("path", [("from", 1), ("to", 4)]),
                Fact::new("path", [("from", 2), ("to", 3)]),
                Fact::new("path", [("from", 2), ("to", 4)]),
                Fact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }

    #[test]
    fn test_source_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        input edge(from, to).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let mut vm: VM = VM::new(ast);
        let tx = vm.input_channel();
        let mut rx = vm.output_channel("path");

        for fact in vec![
            Fact::new("edge", [("from", 0), ("to", 1)]),
            Fact::new("edge", [("from", 1), ("to", 2)]),
            Fact::new("edge", [("from", 2), ("to", 3)]),
            Fact::new("edge", [("from", 3), ("to", 4)]),
        ] {
            let _ = tx.unbounded_send(fact);
        }

        vm.step_epoch()?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = rx.try_next() {
            path.insert(fact);
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                Fact::new("path", [("from", 0), ("to", 1)]),
                Fact::new("path", [("from", 0), ("to", 2)]),
                Fact::new("path", [("from", 0), ("to", 3)]),
                Fact::new("path", [("from", 0), ("to", 4)]),
                Fact::new("path", [("from", 1), ("to", 2)]),
                Fact::new("path", [("from", 1), ("to", 3)]),
                Fact::new("path", [("from", 1), ("to", 4)]),
                Fact::new("path", [("from", 2), ("to", 3)]),
                Fact::new("path", [("from", 2), ("to", 4)]),
                Fact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }

    #[test]
    fn test_sink_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        edge(from: 0, to: 1).
        edge(from: 1, to: 2).
        edge(from: 2, to: 3).
        edge(from: 3, to: 4).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let mut vm: VM = VM::new(ast);
        let mut rx = vm.output_channel("path");

        vm.step_epoch()?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = rx.try_next() {
            path.insert(fact);
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                Fact::new("path", [("from", 0), ("to", 1)]),
                Fact::new("path", [("from", 0), ("to", 2)]),
                Fact::new("path", [("from", 0), ("to", 3)]),
                Fact::new("path", [("from", 0), ("to", 4)]),
                Fact::new("path", [("from", 1), ("to", 2)]),
                Fact::new("path", [("from", 1), ("to", 3)]),
                Fact::new("path", [("from", 1), ("to", 4)]),
                Fact::new("path", [("from", 2), ("to", 3)]),
                Fact::new("path", [("from", 2), ("to", 4)]),
                Fact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }
}

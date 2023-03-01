use core::fmt::Debug;
use std::collections::VecDeque;

use anyhow::Result;
use cid::Cid;
use im::HashMap as ImmHashMap;

use crate::{
    datum::Datum,
    fact::{
        traits::{EDBFact, Fact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    id::{AttributeId, LinkId, VariableId},
    relation::{DefaultRelation, Relation},
    storage::{blockstore::Blockstore, DefaultCodec},
    timestamp::{DefaultTimestamp, Timestamp},
};

use super::ast::{Exit, Loop, Merge, Purge, Swap, *};

type Bindings = ImmHashMap<BindingKey, Datum>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum BindingKey {
    Variable(VariableId),
    Relation(RelationBinding, AttributeId),
}

pub struct VM<
    T = DefaultTimestamp,
    EF = DefaultEDBFact,
    IF = DefaultIDBFact,
    ER = DefaultRelation<EF>,
    IR = DefaultRelation<IF>,
> {
    timestamp: T,
    pc: (usize, Option<usize>),
    input: VecDeque<EF>,
    output: VecDeque<IF>,
    program: Program,
    // TODO: Better data structure
    edb: ImmHashMap<RelationRef, ER>,
    idb: ImmHashMap<RelationRef, IR>,
}

impl<T, EF, IF, ER, IR> Debug for VM<T, EF, IF, ER, IR>
where
    T: Timestamp,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VM")
            .field("timestamp", &self.timestamp)
            .field("pc", &self.pc)
            .finish()
    }
}

impl<T, EF, IF, ER, IR> VM<T, EF, IF, ER, IR>
where
    T: Timestamp,
    ER: Relation<EF>,
    IR: Relation<IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    pub fn new(program: Program) -> Self {
        let mut edb = ImmHashMap::<RelationRef, ER>::default();
        for input in program.inputs() {
            for version in [
                RelationVersion::New,
                RelationVersion::Delta,
                RelationVersion::Total,
            ] {
                let relation_ref = RelationRef::new(*input, RelationSource::EDB, version);

                edb.insert(relation_ref, ER::default());
            }
        }

        let mut idb = ImmHashMap::<RelationRef, IR>::default();
        for output in program.outputs() {
            for version in [
                RelationVersion::New,
                RelationVersion::Delta,
                RelationVersion::Total,
            ] {
                let relation_ref = RelationRef::new(*output, RelationSource::IDB, version);

                idb.insert(relation_ref, IR::default());
            }
        }

        Self {
            timestamp: T::default(),
            pc: (0, None),
            input: VecDeque::default(),
            output: VecDeque::default(),
            program,
            edb,
            idb,
        }
    }

    pub fn timestamp(&self) -> &T {
        &self.timestamp
    }

    pub fn relation(&self, id: &str) -> IR {
        self.idb
            .get(&RelationRef::new(
                id,
                RelationSource::IDB,
                RelationVersion::Total,
            ))
            .cloned()
            .unwrap_or_default()
    }

    pub fn push(&mut self, fact: EF) -> Result<()> {
        self.input.push_back(fact);

        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<IF>> {
        let fact = self.output.pop_front();

        Ok(fact)
    }

    pub fn step_epoch<BS>(&mut self, blockstore: &BS) -> Result<()>
    where
        BS: Blockstore,
    {
        assert!(self.timestamp == self.timestamp.epoch_start());

        let mut has_new_facts = false;

        while let Some(fact) = self.input.pop_front() {
            let relation_ref =
                RelationRef::new(fact.id(), RelationSource::EDB, RelationVersion::Delta);

            self.edb = self.edb.alter(
                |old| match old {
                    Some(facts) => Some(facts.insert(fact)),
                    None => Some(ER::default().insert(fact)),
                },
                relation_ref,
            );

            has_new_facts = true;
        }

        let start = self.timestamp;

        // Only run the epoch if there's new input facts
        // TODO: We also run once at the start, to handle hardcoded facts. This isn't
        // super elegant though.
        if has_new_facts || start == self.timestamp().clock_start() {
            loop {
                self.step(blockstore)?;

                if self.timestamp.epoch() != start.epoch() {
                    break;
                }
            }
        }

        Ok(())
    }

    fn step<BS>(&mut self, blockstore: &BS) -> Result<()>
    where
        BS: Blockstore,
    {
        match self.load_statement().clone() {
            Statement::Insert(insert) => self.handle_insert(&insert, blockstore),
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

    fn handle_insert<BS>(&mut self, insert: &Insert, blockstore: &BS)
    where
        BS: Blockstore,
    {
        // Only insert ground facts on the first clock cycle
        if insert.is_ground() && *self.timestamp() != self.timestamp().clock_start() {
        } else {
            self.handle_operation(insert.operation(), blockstore);
        }
    }

    fn handle_operation<BS>(&mut self, operation: &Operation, blockstore: &BS)
    where
        BS: Blockstore,
    {
        let bindings = Bindings::default();

        self.do_handle_operation(operation, blockstore, &bindings)
    }

    fn do_handle_operation<BS>(
        &mut self,
        operation: &Operation,
        blockstore: &BS,
        bindings: &Bindings,
    ) where
        BS: Blockstore,
    {
        match operation {
            Operation::Search(inner) => self.handle_search(inner, blockstore, bindings),
            Operation::Project(inner) => self.handle_project(inner, blockstore, bindings),
            Operation::GetLink(inner) => self.handle_get_link(inner, blockstore, bindings),
        }
    }

    fn handle_search<BS>(&mut self, search: &Search, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        match search.relation().source() {
            RelationSource::EDB => {
                let to_search = self.edb.get(search.relation()).cloned().unwrap();

                self.search_relation(search, blockstore, to_search, bindings);
            }
            RelationSource::IDB => {
                let to_search = self.idb.get(search.relation()).cloned().unwrap();

                self.search_relation(search, blockstore, to_search, bindings);
            }
        };
    }

    fn search_relation<BS, R, F>(
        &mut self,
        search: &Search,
        blockstore: &BS,
        to_search: R,
        bindings: &Bindings,
    ) where
        BS: Blockstore,
        R: Relation<F>,
        F: Fact,
    {
        let relation_binding = RelationBinding::new(
            search.relation().id(),
            search.relation().source(),
            *search.alias(),
        );

        for fact in &mut to_search.into_iter() {
            let mut next_bindings = bindings.clone();

            for (k, v) in fact.attributes() {
                next_bindings.insert(BindingKey::Relation(relation_binding, k), v);
            }

            let is_satisfied = search.when().iter().all(|f| match f {
                Formula::Equality(equality) => {
                    let left = match equality.left() {
                        Term::Attribute(inner) if *inner.relation() == relation_binding => {
                            fact.attribute(inner.id())
                        }
                        Term::Attribute(inner) => bindings
                            .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                            .cloned(),
                        Term::Variable(inner) => {
                            bindings.get(&BindingKey::Variable(*inner.id())).cloned()
                        }
                        Term::Literal(inner) => Some(*inner.datum()),
                    };

                    let right = match equality.right() {
                        Term::Attribute(inner) if *inner.relation() == relation_binding => {
                            fact.attribute(inner.id())
                        }
                        Term::Attribute(inner) => bindings
                            .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                            .cloned(),
                        Term::Variable(inner) => {
                            bindings.get(&BindingKey::Variable(*inner.id())).cloned()
                        }
                        Term::Literal(inner) => Some(*inner.datum()),
                    };

                    left == right
                }
                Formula::NotIn(not_in) => {
                    // TODO: Dry up constructing a fact from BTreeMap<AttributeId, Term>
                    let mut bound: Vec<(AttributeId, Datum)> = Vec::default();

                    for (id, term) in not_in.attributes() {
                        match term {
                            Term::Attribute(inner) => {
                                let value = next_bindings
                                    .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                                    .unwrap();

                                bound.push((*id, *value));
                            }
                            Term::Variable(inner) => {
                                let value = next_bindings
                                    .get(&BindingKey::Variable(*inner.id()))
                                    .unwrap();

                                bound.push((*id, *value));
                            }
                            Term::Literal(inner) => bound.push((*id, *inner.datum())),
                        }
                    }

                    let bound_fact = IF::new(not_in.relation().id(), bound);

                    !self
                        .idb
                        .get(not_in.relation())
                        .map(|r| r.contains(&bound_fact))
                        .unwrap()
                }
            });

            if !is_satisfied {
                continue;
            }

            self.do_handle_operation(search.operation(), blockstore, &next_bindings);
        }
    }

    fn handle_project<BS>(&mut self, project: &Project, _blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        let mut bound: Vec<(AttributeId, Datum)> = Vec::default();

        for (id, term) in project.attributes() {
            match term {
                Term::Attribute(inner) => {
                    let value = bindings
                        .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                        .unwrap();

                    bound.push((*id, *value));
                }
                Term::Variable(inner) => {
                    let value = bindings.get(&BindingKey::Variable(*inner.id())).unwrap();

                    bound.push((*id, *value));
                }
                Term::Literal(literal) => bound.push((*id, *literal.datum())),
            }
        }

        match project.into().source() {
            RelationSource::EDB => {
                let fact = EF::new(project.into().id(), bound, Vec::<(LinkId, Cid)>::default());

                self.edb = self.edb.alter(
                    |old| match old {
                        Some(facts) => Some(facts.insert(fact)),
                        None => Some(ER::default().insert(fact)),
                    },
                    *project.into(),
                )
            }
            RelationSource::IDB => {
                let fact = IF::new(project.into().id(), bound);

                self.idb = self.idb.alter(
                    |old| match old {
                        Some(facts) => Some(facts.insert(fact)),
                        None => Some(IR::default().insert(fact)),
                    },
                    *project.into(),
                )
            }
        }
    }

    fn handle_get_link<BS>(&mut self, get_link: &GetLink, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        let datum = match get_link.cid_term() {
            Term::Attribute(inner) => bindings
                .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                .cloned(),
            Term::Variable(inner) => bindings.get(&BindingKey::Variable(*inner.id())).cloned(),
            Term::Literal(inner) => Some(*inner.datum()),
        };

        match datum {
            Some(Datum::Cid(cid)) => match blockstore.get_serializable::<DefaultCodec, EF>(&cid) {
                Ok(Some(fact)) => match fact.link(*get_link.link_id()) {
                    Some(link_cid) => match get_link.link_value() {
                        Term::Attribute(inner) => {
                            match bindings
                                .get(&BindingKey::Relation(*inner.relation(), *inner.id()))
                            {
                                Some(value) => {
                                    if *value == Datum::cid(*link_cid) {
                                        self.do_handle_operation(
                                            get_link.operation(),
                                            blockstore,
                                            bindings,
                                        );
                                    } else {
                                    }
                                }
                                None => {
                                    let bindings = bindings.update(
                                        BindingKey::Relation(*inner.relation(), *inner.id()),
                                        Datum::cid(*link_cid),
                                    );

                                    self.do_handle_operation(
                                        get_link.operation(),
                                        blockstore,
                                        &bindings,
                                    );
                                }
                            }
                        }
                        Term::Variable(inner) => {
                            match bindings.get(&BindingKey::Variable(*inner.id())) {
                                Some(value) => {
                                    if *value == Datum::cid(*link_cid) {
                                        self.do_handle_operation(
                                            get_link.operation(),
                                            blockstore,
                                            bindings,
                                        );
                                    } else {
                                    }
                                }
                                None => {
                                    let bindings = bindings.update(
                                        BindingKey::Variable(*inner.id()),
                                        Datum::cid(*link_cid),
                                    );

                                    self.do_handle_operation(
                                        get_link.operation(),
                                        blockstore,
                                        &bindings,
                                    );
                                }
                            }
                        }
                        Term::Literal(inner) => {
                            if *inner.datum() == Datum::cid(*link_cid) {
                                self.do_handle_operation(
                                    get_link.operation(),
                                    blockstore,
                                    bindings,
                                );
                            } else {
                            }
                        }
                    },
                    None => (),
                },
                Ok(None) => todo!(),
                Err(_) => todo!(),
            },
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    fn handle_merge(&mut self, merge: &Merge) {
        assert!(merge.from().source() == merge.into().source());

        match (merge.from().source(), merge.into().source()) {
            (RelationSource::EDB, RelationSource::EDB) => {
                let from_relation = self.edb.get(merge.from()).cloned().unwrap();

                self.edb = self
                    .edb
                    .update_with(*merge.into(), from_relation, |old, new| old.merge(new));
            }
            (RelationSource::IDB, RelationSource::IDB) => {
                let from_relation = self.idb.get(merge.from()).cloned().unwrap();

                self.idb = self
                    .idb
                    .update_with(*merge.into(), from_relation, |old, new| old.merge(new));
            }
            _ => unreachable!(),
        }
    }

    fn handle_swap(&mut self, swap: &Swap) {
        assert!(swap.left().source() == swap.right().source());

        match (swap.left().source(), swap.right().source()) {
            (RelationSource::EDB, RelationSource::EDB) => {
                let left_relation = self.edb.remove(swap.left()).unwrap();
                let right_relation = self.edb.remove(swap.right()).unwrap();

                self.edb = self.edb.update(*swap.left(), right_relation);
                self.edb = self.edb.update(*swap.right(), left_relation);
            }
            (RelationSource::IDB, RelationSource::IDB) => {
                let left_relation = self.idb.remove(swap.left()).unwrap();
                let right_relation = self.idb.remove(swap.right()).unwrap();

                self.idb = self.idb.update(*swap.left(), right_relation);
                self.idb = self.idb.update(*swap.right(), left_relation);
            }
            _ => unreachable!(),
        }
    }

    fn handle_purge(&mut self, purge: &Purge) {
        match purge.relation().source() {
            RelationSource::EDB => self.edb = self.edb.update(*purge.relation(), ER::default()),
            RelationSource::IDB => self.idb = self.idb.update(*purge.relation(), IR::default()),
        }
    }

    fn handle_exit(&mut self, exit: &Exit) {
        let is_done = exit.relations().iter().all(|r| match r.source() {
            RelationSource::EDB => self.edb.get(r).map_or(false, ER::is_empty),
            RelationSource::IDB => self.idb.get(r).map_or(false, IR::is_empty),
        });

        if is_done {
            self.pc.1 = None;
        }
    }

    fn handle_sources(&mut self, _sources: &Sources) -> Result<()> {
        // TODO: Channels are now read at the start of the epoch; maybe remove this?

        Ok(())
    }

    fn handle_sinks(&mut self, sinks: &Sinks) -> Result<()> {
        for relation_ref in sinks.relations() {
            assert!(relation_ref.source() == RelationSource::IDB);

            if let Some(relation) = self.idb.get(relation_ref) {
                for fact in relation.clone() {
                    self.output.push_back(fact);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        fact::traits::{EDBFact, IDBFact},
        logic::{lower_to_ram, parser},
        storage::memory::MemoryBlockstore,
    };
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        input evac(entity, attribute, value).

        output edge(from, to).
        output path(from, to).

        evac(entity: 0, attribute: "to", value: 1).
        evac(entity: 1, attribute: "to", value: 2).
        evac(entity: 2, attribute: "to", value: 3).
        evac(entity: 3, attribute: "to", value: 4).

        edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let bs = MemoryBlockstore::default();
        let mut vm: VM = VM::new(ast);

        vm.step_epoch(&bs)?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = vm.pop() {
            if fact.id() == "path".into() {
                path.insert(fact);
            }
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                IDBFact::new("path", [("from", 0), ("to", 1)],),
                IDBFact::new("path", [("from", 0), ("to", 2)],),
                IDBFact::new("path", [("from", 0), ("to", 3)],),
                IDBFact::new("path", [("from", 0), ("to", 4)],),
                IDBFact::new("path", [("from", 1), ("to", 2)],),
                IDBFact::new("path", [("from", 1), ("to", 3)],),
                IDBFact::new("path", [("from", 1), ("to", 4)],),
                IDBFact::new("path", [("from", 2), ("to", 3)],),
                IDBFact::new("path", [("from", 2), ("to", 4)],),
                IDBFact::new("path", [("from", 3), ("to", 4)],),
            ])
        );

        Ok(())
    }

    #[test]
    fn test_source_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        input evac(entity, attribute, value).

        output edge(from, to).
        output path(from, to).

        edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let bs = MemoryBlockstore::default();
        let mut vm: VM = VM::new(ast);

        for fact in vec![
            EDBFact::new(
                "evac",
                [
                    ("entity", Datum::int(0)),
                    ("attribute", Datum::string("to")),
                    ("value", Datum::int(1)),
                ],
                Vec::<(LinkId, Cid)>::default(),
            ),
            EDBFact::new(
                "evac",
                [
                    ("entity", Datum::int(1)),
                    ("attribute", Datum::string("to")),
                    ("value", Datum::int(2)),
                ],
                Vec::<(LinkId, Cid)>::default(),
            ),
            EDBFact::new(
                "evac",
                [
                    ("entity", Datum::int(2)),
                    ("attribute", Datum::string("to")),
                    ("value", Datum::int(3)),
                ],
                Vec::<(LinkId, Cid)>::default(),
            ),
            EDBFact::new(
                "evac",
                [
                    ("entity", Datum::int(3)),
                    ("attribute", Datum::string("to")),
                    ("value", Datum::int(4)),
                ],
                Vec::<(LinkId, Cid)>::default(),
            ),
        ] {
            let _ = vm.push(fact);
        }

        vm.step_epoch(&bs)?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = vm.pop() {
            if fact.id() == "path".into() {
                path.insert(fact);
            }
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                IDBFact::new("path", [("from", 0), ("to", 1)]),
                IDBFact::new("path", [("from", 0), ("to", 2)]),
                IDBFact::new("path", [("from", 0), ("to", 3)]),
                IDBFact::new("path", [("from", 0), ("to", 4)]),
                IDBFact::new("path", [("from", 1), ("to", 2)]),
                IDBFact::new("path", [("from", 1), ("to", 3)]),
                IDBFact::new("path", [("from", 1), ("to", 4)]),
                IDBFact::new("path", [("from", 2), ("to", 3)]),
                IDBFact::new("path", [("from", 2), ("to", 4)]),
                IDBFact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }

    #[test]
    fn test_sink_transitive_closure() -> Result<()> {
        let program = parser::parse(
            r#"
        input evac(entity, attribute, value).

        output edge(from, to).
        output path(from, to).

        evac(entity: 0, attribute: "to", value: 1).
        evac(entity: 1, attribute: "to", value: 2).
        evac(entity: 2, attribute: "to", value: 3).
        evac(entity: 3, attribute: "to", value: 4).

        edge(from: X, to: Y) :- evac(entity: X, attribute: "to", value: Y).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )?;

        let ast = lower_to_ram::lower_to_ram(&program)?;
        let bs = MemoryBlockstore::default();
        let mut vm: VM = VM::new(ast);

        vm.step_epoch(&bs)?;

        let mut path = BTreeSet::default();
        while let Ok(Some(fact)) = vm.pop() {
            if fact.id() == "path".into() {
                path.insert(fact);
            }
        }

        assert_eq!(
            path,
            BTreeSet::from_iter([
                IDBFact::new("path", [("from", 0), ("to", 1)]),
                IDBFact::new("path", [("from", 0), ("to", 2)]),
                IDBFact::new("path", [("from", 0), ("to", 3)]),
                IDBFact::new("path", [("from", 0), ("to", 4)]),
                IDBFact::new("path", [("from", 1), ("to", 2)]),
                IDBFact::new("path", [("from", 1), ("to", 3)]),
                IDBFact::new("path", [("from", 1), ("to", 4)]),
                IDBFact::new("path", [("from", 2), ("to", 3)]),
                IDBFact::new("path", [("from", 2), ("to", 4)]),
                IDBFact::new("path", [("from", 3), ("to", 4)]),
            ])
        );

        Ok(())
    }
}

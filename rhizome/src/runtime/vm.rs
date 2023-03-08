use core::fmt::Debug;
use std::collections::VecDeque;

use anyhow::Result;

use crate::{
    fact::{
        traits::{EDBFact, Fact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    id::{ColumnId, RelationId, VarId},
    ram::{
        formula::Formula,
        operation::{get_link::GetLink, project::Project, search::Search, Operation},
        program::Program,
        relation_binding::RelationBinding,
        relation_ref::RelationRef,
        relation_version::RelationVersion,
        statement::{
            exit::Exit, insert::Insert, merge::Merge, purge::Purge, recursive::Loop, sinks::Sinks,
            sources::Sources, swap::Swap, Statement,
        },
        term::Term,
    },
    relation::{DefaultRelation, Relation},
    storage::{blockstore::Blockstore, DefaultCodec},
    timestamp::{DefaultTimestamp, Timestamp},
    value::Value,
};

type Bindings = im::HashMap<BindingKey, Value>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum BindingKey {
    Variable(VarId),
    Relation(RelationBinding, ColumnId),
}

pub struct VM<
    T = DefaultTimestamp,
    EF = DefaultEDBFact,
    IF = DefaultIDBFact,
    ER = DefaultRelation<EF>,
    IR = DefaultRelation<IF>,
> where
    EF: EDBFact,
    IF: IDBFact,
{
    timestamp: T,
    pc: (usize, Option<usize>),
    input: VecDeque<EF>,
    output: VecDeque<IF>,
    program: Program,
    // TODO: Better data structure
    edb: im::HashMap<(RelationId, RelationVersion), ER>,
    idb: im::HashMap<(RelationId, RelationVersion), IR>,
}

impl<T, EF, IF, ER, IR> Debug for VM<T, EF, IF, ER, IR>
where
    T: Timestamp,
    ER: Relation<EF>,
    IR: Relation<IF>,
    EF: EDBFact,
    IF: IDBFact,
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
        let mut edb = im::HashMap::<(RelationId, RelationVersion), ER>::default();
        for input in program.inputs() {
            for version in [
                RelationVersion::New,
                RelationVersion::Delta,
                RelationVersion::Total,
            ] {
                edb.insert((input.id(), version), ER::default());
            }
        }

        let mut idb = im::HashMap::<(RelationId, RelationVersion), IR>::default();
        for output in program.outputs() {
            for version in [
                RelationVersion::New,
                RelationVersion::Delta,
                RelationVersion::Total,
            ] {
                idb.insert((output.id(), version), IR::default());
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

    pub fn relation<S>(&self, id: S) -> IR
    where
        S: AsRef<str>,
    {
        self.idb
            .get(&(RelationId::new(id), RelationVersion::Total))
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
            let id = fact.id();

            self.edb = self.edb.alter(
                |old| match old {
                    Some(facts) => Some(facts.insert(fact)),
                    None => Some(ER::default().insert(fact)),
                },
                (id, RelationVersion::Delta),
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
        match *search.relation() {
            RelationRef::EDB(inner) => {
                let id = inner.id();
                let version = inner.version();

                let to_search = self.edb.get(&(id, version)).cloned().unwrap();
                let relation_binding = RelationBinding::edb(id, *search.alias());

                self.search_relation(search, blockstore, to_search, relation_binding, bindings);
            }
            RelationRef::IDB(inner) => {
                let id = inner.id();
                let version = inner.version();

                let to_search = self.idb.get(&(id, version)).cloned().unwrap();
                let relation_binding = RelationBinding::idb(id, *search.alias());

                self.search_relation(search, blockstore, to_search, relation_binding, bindings);
            }
        };
    }

    fn search_relation<BS, R, F>(
        &mut self,
        search: &Search,
        blockstore: &BS,
        to_search: R,
        relation_binding: RelationBinding,
        bindings: &Bindings,
    ) where
        BS: Blockstore,
        R: Relation<F>,
        F: Fact,
    {
        for fact in &mut to_search.into_iter() {
            let mut next_bindings = bindings.clone();

            for (k, v) in fact.attributes() {
                next_bindings.insert(BindingKey::Relation(relation_binding, k), v);
            }

            let is_satisfied = search.when().iter().all(|f| match f {
                Formula::Equality(equality) => {
                    let left = match equality.left() {
                        Term::Attribute(column, column_binding)
                            if *column_binding == relation_binding =>
                        {
                            fact.attribute(column)
                        }
                        Term::Attribute(column, column_binding) => bindings
                            .get(&BindingKey::Relation(*column_binding, *column))
                            .cloned(),
                        Term::Variable(variable) => {
                            bindings.get(&BindingKey::Variable(*variable)).cloned()
                        }
                        Term::Literal(value) => Some(value.clone()),
                    };

                    let right = match equality.right() {
                        Term::Attribute(column, column_binding)
                            if *column_binding == relation_binding =>
                        {
                            fact.attribute(column)
                        }
                        Term::Attribute(column, column_binding) => bindings
                            .get(&BindingKey::Relation(*column_binding, *column))
                            .cloned(),
                        Term::Variable(variable) => {
                            bindings.get(&BindingKey::Variable(*variable)).cloned()
                        }
                        Term::Literal(value) => Some(value.clone()),
                    };

                    left == right
                }
                Formula::NotIn(not_in) => {
                    // TODO: Dry up constructing a fact from BTreeMap<AttributeId, Term>
                    let mut bound: Vec<(ColumnId, Value)> = Vec::default();

                    for (id, term) in not_in.attributes() {
                        match term {
                            Term::Attribute(column, column_binding) => {
                                let value = next_bindings
                                    .get(&BindingKey::Relation(*column_binding, *column))
                                    .unwrap();

                                bound.push((*id, value.clone()));
                            }
                            Term::Variable(variable) => {
                                let value =
                                    next_bindings.get(&BindingKey::Variable(*variable)).unwrap();

                                bound.push((*id, value.clone()));
                            }
                            Term::Literal(value) => bound.push((*id, value.clone())),
                        }
                    }

                    match *not_in.relation() {
                        RelationRef::EDB(_) => unreachable!(),
                        RelationRef::IDB(inner) => {
                            let bound_fact = IF::new(inner.id(), bound);

                            !self
                                .idb
                                .get(&(inner.id(), inner.version()))
                                .map(|r| r.contains(&bound_fact))
                                .unwrap()
                        }
                    }
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
        let mut bound: Vec<(ColumnId, Value)> = Vec::default();

        for (id, term) in project.attributes() {
            match term {
                Term::Attribute(column, column_binding) => {
                    let value = bindings
                        .get(&BindingKey::Relation(*column_binding, *column))
                        .unwrap();

                    bound.push((*id, value.clone()));
                }
                Term::Variable(variable) => {
                    let value = bindings.get(&BindingKey::Variable(*variable)).unwrap();

                    bound.push((*id, value.clone()));
                }
                Term::Literal(value) => bound.push((*id, value.clone())),
            }
        }

        match *project.into() {
            RelationRef::EDB(_) => {
                unreachable!();
            }
            RelationRef::IDB(inner) => {
                let fact = IF::new(inner.id(), bound);

                self.idb = self.idb.alter(
                    |old| match old {
                        Some(facts) => Some(facts.insert(fact)),
                        None => Some(IR::default().insert(fact)),
                    },
                    (inner.id(), inner.version()),
                )
            }
        }
    }

    fn handle_get_link<BS>(&mut self, get_link: &GetLink, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        let value = match get_link.cid_term() {
            Term::Attribute(column, column_binding) => bindings
                .get(&BindingKey::Relation(*column_binding, *column))
                .cloned(),
            Term::Variable(variable) => bindings.get(&BindingKey::Variable(*variable)).cloned(),
            Term::Literal(value) => Some(value.clone()),
        };

        match value {
            Some(Value::Cid(cid)) => match blockstore.get_serializable::<DefaultCodec, EF>(&cid) {
                Ok(Some(fact)) => match fact.link(*get_link.link_id()) {
                    Some(link_cid) => match get_link.link_value() {
                        Term::Attribute(column, column_binding) => {
                            let binding_key = BindingKey::Relation(*column_binding, *column);

                            match bindings.get(&binding_key) {
                                Some(value) => {
                                    if *value == Value::Cid(*link_cid) {
                                        self.do_handle_operation(
                                            get_link.operation(),
                                            blockstore,
                                            bindings,
                                        );
                                    } else {
                                    }
                                }
                                None => {
                                    let bindings =
                                        bindings.update(binding_key, Value::Cid(*link_cid));

                                    self.do_handle_operation(
                                        get_link.operation(),
                                        blockstore,
                                        &bindings,
                                    );
                                }
                            }
                        }
                        Term::Variable(variable) => {
                            match bindings.get(&BindingKey::Variable(*variable)) {
                                Some(value) => {
                                    if *value == Value::Cid(*link_cid) {
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
                                        BindingKey::Variable(*variable),
                                        Value::Cid(*link_cid),
                                    );

                                    self.do_handle_operation(
                                        get_link.operation(),
                                        blockstore,
                                        &bindings,
                                    );
                                }
                            }
                        }
                        Term::Literal(value) => {
                            if *value == Value::Cid(*link_cid) {
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
        match (*merge.from(), *merge.into()) {
            (RelationRef::EDB(from_inner), RelationRef::EDB(to_inner)) => {
                let from_relation = self
                    .edb
                    .get(&(from_inner.id(), from_inner.version()))
                    .cloned()
                    .unwrap();

                self.edb = self.edb.update_with(
                    (to_inner.id(), to_inner.version()),
                    from_relation,
                    |old, new| old.merge(new),
                );
            }
            (RelationRef::IDB(from_inner), RelationRef::IDB(to_inner)) => {
                let from_relation = self
                    .idb
                    .get(&(from_inner.id(), from_inner.version()))
                    .cloned()
                    .unwrap();

                self.idb = self.idb.update_with(
                    (to_inner.id(), to_inner.version()),
                    from_relation,
                    |old, new| old.merge(new),
                )
            }
            _ => unreachable!(),
        }
    }

    fn handle_swap(&mut self, swap: &Swap) {
        match (*swap.left(), *swap.right()) {
            (RelationRef::EDB(left_inner), RelationRef::EDB(right_inner)) => {
                let left_relation = self
                    .edb
                    .remove(&(left_inner.id(), left_inner.version()))
                    .unwrap();
                let right_relation = self
                    .edb
                    .remove(&(right_inner.id(), right_inner.version()))
                    .unwrap();

                self.edb = self
                    .edb
                    .update((left_inner.id(), left_inner.version()), right_relation);
                self.edb = self
                    .edb
                    .update((right_inner.id(), right_inner.version()), left_relation);
            }

            (RelationRef::IDB(left_inner), RelationRef::IDB(right_inner)) => {
                let left_relation = self
                    .idb
                    .remove(&(left_inner.id(), left_inner.version()))
                    .unwrap();
                let right_relation = self
                    .idb
                    .remove(&(right_inner.id(), right_inner.version()))
                    .unwrap();

                self.idb = self
                    .idb
                    .update((left_inner.id(), left_inner.version()), right_relation);
                self.idb = self
                    .idb
                    .update((right_inner.id(), right_inner.version()), left_relation)
            }
            _ => unreachable!(),
        }
    }

    fn handle_purge(&mut self, purge: &Purge) {
        match *purge.relation() {
            RelationRef::EDB(inner) => {
                self.edb = self
                    .edb
                    .update((inner.id(), inner.version()), ER::default())
            }
            RelationRef::IDB(inner) => {
                self.idb = self
                    .idb
                    .update((inner.id(), inner.version()), IR::default())
            }
        }
    }

    fn handle_exit(&mut self, exit: &Exit) {
        let is_done = exit.relations().iter().all(|r| match *r {
            RelationRef::EDB(inner) => self
                .edb
                .get(&(inner.id(), inner.version()))
                .map_or(false, ER::is_empty),
            RelationRef::IDB(inner) => self
                .idb
                .get(&(inner.id(), inner.version()))
                .map_or(false, IR::is_empty),
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
        for &relation_ref in sinks.relations() {
            let RelationRef::IDB(inner) = relation_ref else {
                unreachable!();
            };

            if let Some(relation) = self.idb.get(&(inner.id(), inner.version())) {
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
    use crate::{assert_derives, fact::traits::IDBFact, types::Any};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_step_epoch_transitive_closure() {
        assert_derives!(
            "path",
            |p| {
                p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
                p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

                p.fact("edge", |f| f.bind((("from", 0), ("to", 1))))?;
                p.fact("edge", |f| f.bind((("from", 1), ("to", 2))))?;
                p.fact("edge", |f| f.bind((("from", 2), ("to", 3))))?;
                p.fact("edge", |f| f.bind((("from", 3), ("to", 4))))?;

                p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                    (
                        h.bind((("from", x), ("to", y))),
                        b.search("edge", (("from", x), ("to", y))),
                    )
                })?;

                p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                    (
                        h.bind((("from", x), ("to", z))),
                        b.search("edge", (("from", x), ("to", y)))
                            .search("path", (("from", y), ("to", z))),
                    )
                })
            },
            [
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
            ]
        );
    }

    #[test]
    fn test_source_transitive_closure() {
        assert_derives!(
            "path",
            |p| {
                p.input("evac", |h| {
                    h.column::<Any>("entity")
                        .column::<Any>("attribute")
                        .column::<Any>("value")
                })?;

                p.output("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
                p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

                p.fact("edge", |f| f.bind((("from", 0), ("to", 1))))?;
                p.fact("edge", |f| f.bind((("from", 1), ("to", 2))))?;
                p.fact("edge", |f| f.bind((("from", 2), ("to", 3))))?;
                p.fact("edge", |f| f.bind((("from", 3), ("to", 4))))?;

                p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                    (
                        h.bind((("from", x), ("to", y))),
                        b.search("edge", (("from", x), ("to", y))),
                    )
                })?;

                p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                    (
                        h.bind((("from", x), ("to", z))),
                        b.search("edge", (("from", x), ("to", y)))
                            .search("path", (("from", y), ("to", z))),
                    )
                })
            },
            [
                EDBFact::new(0, "to", 1, vec![]),
                EDBFact::new(1, "to", 2, vec![]),
                EDBFact::new(2, "to", 3, vec![]),
                EDBFact::new(3, "to", 4, vec![]),
            ],
            [
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
            ]
        );
    }
}

use core::fmt::Debug;
use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;

use crate::{
    fact::{
        traits::{EDBFact, Fact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    id::{ColId, RelationId},
    ram::{
        formula::Formula,
        operation::{project::Project, search::Search, Operation},
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
    value::Val,
};

type Bindings = im::HashMap<BindingKey, Arc<Val>>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum BindingKey {
    Relation(RelationBinding, ColId),
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

    pub fn relation<S>(&self, id: S) -> &IR
    where
        S: AsRef<str>,
    {
        self.idb
            .get(&(RelationId::new(id), RelationVersion::Total))
            .unwrap()
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
        match &*self.load_statement() {
            Statement::Insert(insert) => self.handle_insert(insert, blockstore),
            Statement::Merge(merge) => self.handle_merge(merge),
            Statement::Swap(swap) => self.handle_swap(swap),
            Statement::Purge(purge) => self.handle_purge(purge),
            Statement::Exit(exit) => {
                assert!(self.pc.1.is_some());

                self.handle_exit(exit);
            }
            Statement::Sources(sources) => self.handle_sources(sources)?,
            Statement::Sinks(sinks) => self.handle_sinks(sinks)?,
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
                if let Some(statement) = self.program.statements().get(self.pc.0 + 1) {
                    if let Statement::Loop(Loop { .. }) = &**statement {
                        ((outer + 1) % self.program.statements().len(), Some(0))
                    } else {
                        ((outer + 1) % self.program.statements().len(), None)
                    }
                } else {
                    ((outer + 1) % self.program.statements().len(), None)
                }
            }
            (outer, Some(inner)) => {
                if let Statement::Loop(loop_statement) =
                    &**self.program.statements().get(self.pc.0).unwrap()
                {
                    (outer, Some((inner + 1) % loop_statement.body().len()))
                } else {
                    unreachable!("Current statement must be a loop!")
                }
            }
        }
    }

    fn load_statement(&self) -> Arc<Statement> {
        match &**self.program.statements().get(self.pc.0).unwrap() {
            Statement::Loop(loop_statement) => {
                assert!(self.pc.1.is_some());

                Arc::clone(loop_statement.body().get(self.pc.1.unwrap()).unwrap())
            }
            _ => {
                assert!(self.pc.1.is_none());

                Arc::clone(self.program.statements().get(self.pc.0).unwrap())
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
        }
    }

    fn handle_search<BS>(&mut self, search: &Search, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        match *search.relation() {
            RelationRef::Edb(inner) => {
                let id = inner.id();
                let version = inner.version();

                let to_search = self.edb.get(&(id, version)).cloned().unwrap();
                let relation_binding = RelationBinding::edb(id, *search.alias());

                self.search_relation(search, blockstore, to_search, relation_binding, bindings);
            }
            RelationRef::Idb(inner) => {
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

            for k in fact.cols() {
                if let Some(v) = fact.col(&k) {
                    next_bindings.insert(BindingKey::Relation(relation_binding, k), v.clone());
                } else {
                    panic!("expected column missing: {k}");
                }
            }

            if !self.is_formulae_satisfied(search.when(), blockstore, &next_bindings) {
                continue;
            }

            self.do_handle_operation(search.operation(), blockstore, &next_bindings);
        }
    }

    fn handle_project<BS>(&mut self, project: &Project, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in project.cols() {
            if let Some(val) = Self::resolve_term(term, blockstore, bindings) {
                bound.push((*id, <Val>::clone(&val)));
            } else {
                return;
            }
        }

        match *project.into() {
            RelationRef::Edb(_) => {
                unreachable!();
            }
            RelationRef::Idb(inner) => {
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

    fn handle_merge(&mut self, merge: &Merge) {
        match (*merge.from(), *merge.into()) {
            (RelationRef::Edb(from_inner), RelationRef::Edb(to_inner)) => {
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
            (RelationRef::Idb(from_inner), RelationRef::Idb(to_inner)) => {
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
            (RelationRef::Edb(left_inner), RelationRef::Edb(right_inner)) => {
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

            (RelationRef::Idb(left_inner), RelationRef::Idb(right_inner)) => {
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
            RelationRef::Edb(inner) => {
                self.edb = self
                    .edb
                    .update((inner.id(), inner.version()), ER::default())
            }
            RelationRef::Idb(inner) => {
                self.idb = self
                    .idb
                    .update((inner.id(), inner.version()), IR::default())
            }
        }
    }

    fn handle_exit(&mut self, exit: &Exit) {
        let is_done = exit.relations().iter().all(|r| match *r {
            RelationRef::Edb(inner) => self
                .edb
                .get(&(inner.id(), inner.version()))
                .map_or(false, ER::is_empty),
            RelationRef::Idb(inner) => self
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
            let RelationRef::Idb(inner) = relation_ref else {
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

    fn resolve_term<BS>(term: &Term, blockstore: &BS, bindings: &Bindings) -> Option<Arc<Val>>
    where
        BS: Blockstore,
    {
        match term {
            Term::Link(link_id, cid_term) => {
                let Some(cid_val) = Self::resolve_term(cid_term, blockstore, bindings) else {
                    panic!();
                };

                let Val::Cid(cid) = &*cid_val else {
                    panic!();
                };

                let Ok(Some(fact)) = blockstore.get_serializable::<DefaultCodec, EF>(cid) else {
                        return None;
                    };

                fact.link(*link_id)
            }
            Term::Col(col, col_binding) => bindings
                .get(&BindingKey::Relation(*col_binding, *col))
                .map(Arc::clone),
            Term::Lit(val) => Some(val).map(Arc::clone),
        }
    }

    fn is_formulae_satisfied<BS>(
        &self,
        when: &[Formula],
        blockstore: &BS,
        bindings: &Bindings,
    ) -> bool
    where
        BS: Blockstore,
    {
        when.iter().all(|f| match f {
            Formula::Equality(equality) => {
                let left = Self::resolve_term(equality.left(), blockstore, bindings);
                let right = Self::resolve_term(equality.right(), blockstore, bindings);

                left == right
            }
            Formula::NotIn(not_in) => {
                // TODO: Dry up constructing a fact from BTreeMap<ColId, Term>
                let mut bound: Vec<(ColId, Val)> = Vec::default();

                for (id, term) in not_in.cols() {
                    if let Some(val) = Self::resolve_term(term, blockstore, bindings) {
                        bound.push((*id, <Val>::clone(&val)));
                    }
                }

                match *not_in.relation() {
                    RelationRef::Edb(_) => unreachable!(),
                    RelationRef::Idb(inner) => {
                        let bound_fact = IF::new(inner.id(), bound);

                        !self
                            .idb
                            .get(&(inner.id(), inner.version()))
                            .map(|r| r.contains(&bound_fact))
                            .unwrap()
                    }
                }
            }
            Formula::Predicate(predicate) => {
                let args = predicate
                    .args()
                    .iter()
                    .map(|t| Self::resolve_term(t, blockstore, bindings).unwrap())
                    .map(|v| Arc::try_unwrap(v).unwrap_or_else(|arc| (*arc).clone()))
                    .collect::<Vec<_>>();

                predicate.is_satisfied(args)
            }
        })
    }
}

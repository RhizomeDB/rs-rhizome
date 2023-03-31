use core::fmt::Debug;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

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
        relation_version::RelationVersion,
        statement::{
            exit::Exit, insert::Insert, merge::Merge, purge::Purge, recursive::Loop, sinks::Sinks,
            sources::Sources, swap::Swap, Statement,
        },
        term::Term,
        Reduce,
    },
    relation::{DefaultRelation, Relation, Source},
    storage::{blockstore::Blockstore, DefaultCodec},
    timestamp::{DefaultTimestamp, Timestamp},
    value::Val,
    var::Var,
};

type Bindings = im::HashMap<BindingKey, Arc<Val>>;

// TODO: Put Links in here as they're resolved,
// so that we can memoize their resolution
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum BindingKey {
    Relation(RelationBinding, ColId),
    Agg(RelationBinding, Var),
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
            Operation::Reduce(inner) => self.handle_reduce(inner, blockstore, bindings),
        }
    }

    fn handle_search<BS>(&mut self, search: &Search, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        match search.relation().source() {
            Source::Edb => {
                let id = search.relation().id();
                let version = search.relation().version();

                let to_search = self.edb.get(&(id, version)).cloned().unwrap();
                let relation_binding = RelationBinding::new(id, *search.alias(), Source::Edb);

                self.search_relation(search, blockstore, to_search, relation_binding, bindings);
            }
            Source::Idb => {
                let id = search.relation().id();
                let version = search.relation().version();

                let to_search = self.idb.get(&(id, version)).cloned().unwrap();
                let relation_binding = RelationBinding::new(id, *search.alias(), Source::Idb);

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
        assert!(project.into().source() == Source::Idb);

        let mut bound: Vec<(ColId, Val)> = Vec::default();

        for (id, term) in project.cols() {
            if let Some(val) = Self::resolve_term(term, blockstore, bindings) {
                bound.push((*id, <Val>::clone(&val)));
            } else {
                return;
            }
        }

        let fact = IF::new(project.into().id(), bound);

        self.idb = self.idb.alter(
            |old| match old {
                Some(facts) => Some(facts.insert(fact)),
                None => Some(IR::default().insert(fact)),
            },
            (project.into().id(), project.into().version()),
        )
    }

    fn handle_reduce<BS>(&mut self, agg: &Reduce, blockstore: &BS, bindings: &Bindings)
    where
        BS: Blockstore,
    {
        match agg.relation().source() {
            Source::Edb => {
                let to_search = self
                    .edb
                    .get(&(agg.relation().id(), RelationVersion::Total))
                    .cloned()
                    .unwrap();

                self.do_handle_reduce(agg, blockstore, bindings, to_search);
            }
            Source::Idb => {
                let to_search = self
                    .idb
                    .get(&(agg.relation().id(), RelationVersion::Total))
                    .cloned()
                    .unwrap();

                self.do_handle_reduce(agg, blockstore, bindings, to_search);
            }
        };
    }

    fn do_handle_reduce<BS, R, F>(
        &mut self,
        agg: &Reduce,
        blockstore: &BS,
        bindings: &Bindings,
        to_search: R,
    ) where
        BS: Blockstore,
        R: Relation<F>,
        F: Fact,
    {
        let relation_binding =
            RelationBinding::new(agg.relation().id(), *agg.alias(), agg.relation().source());

        let mut group_by_vals: HashMap<ColId, Arc<Val>> = HashMap::default();
        for (col_id, col_term) in agg.group_by_cols() {
            if let Some(col_val) = Self::resolve_term(col_term, blockstore, bindings) {
                group_by_vals.insert(*col_id, col_val);
            } else {
                panic!();
            };
        }

        // TODO: This is extremely slow. We need indices over the required group_by_cols
        let mut matching: Vec<F> = Vec::default();
        for fact in to_search.into_iter() {
            let mut matches = true;
            for (col_id, col_val) in &group_by_vals {
                if *col_val != fact.col(col_id).unwrap() {
                    matches = false;

                    break;
                }
            }

            if matches {
                matching.push(fact);
            }
        }

        if matching.is_empty() {
            return;
        }

        let result = matching.iter().fold(agg.init().clone(), |acc, f| {
            let mut match_bindings = bindings.clone();

            for k in f.cols() {
                if let Some(v) = f.col(&k) {
                    match_bindings.insert(BindingKey::Relation(relation_binding, k), v.clone());
                } else {
                    panic!("expected column missing: {k}");
                }
            }

            let args = agg
                .args()
                .iter()
                .map(|t| Self::resolve_term(t, blockstore, &match_bindings).unwrap())
                .map(|v| Arc::try_unwrap(v).unwrap_or_else(|arc| (*arc).clone()))
                .collect::<Vec<_>>();

            agg.apply(acc, args)
        });

        let mut next_bindings = bindings.clone();
        next_bindings.insert(
            BindingKey::Agg(relation_binding, agg.target()),
            Arc::new(result),
        );

        self.do_handle_operation(agg.operation(), blockstore, &next_bindings);
    }

    fn handle_merge(&mut self, merge: &Merge) {
        assert!(merge.from().source() == merge.into().source());

        if merge.from().source() == Source::Edb {
            let from_relation = self
                .edb
                .get(&(merge.from().id(), merge.from().version()))
                .cloned()
                .unwrap();

            self.edb = self.edb.update_with(
                (merge.into().id(), merge.into().version()),
                from_relation,
                |old, new| old.merge(new),
            );
        } else {
            let from_relation = self
                .idb
                .get(&(merge.from().id(), merge.from().version()))
                .cloned()
                .unwrap();

            self.idb = self.idb.update_with(
                (merge.into().id(), merge.into().version()),
                from_relation,
                |old, new| old.merge(new),
            )
        }
    }

    fn handle_swap(&mut self, swap: &Swap) {
        assert!(swap.left().source() == swap.right().source());

        if swap.left().source() == Source::Edb {
            let left_relation = self
                .edb
                .remove(&(swap.left().id(), swap.left().version()))
                .unwrap();
            let right_relation = self
                .edb
                .remove(&(swap.right().id(), swap.right().version()))
                .unwrap();

            self.edb = self
                .edb
                .update((swap.left().id(), swap.left().version()), right_relation);
            self.edb = self
                .edb
                .update((swap.right().id(), swap.right().version()), left_relation);
        } else {
            let left_relation = self
                .idb
                .remove(&(swap.left().id(), swap.left().version()))
                .unwrap();
            let right_relation = self
                .idb
                .remove(&(swap.right().id(), swap.right().version()))
                .unwrap();

            self.idb = self
                .idb
                .update((swap.left().id(), swap.left().version()), right_relation);
            self.idb = self
                .idb
                .update((swap.right().id(), swap.right().version()), left_relation)
        }
    }

    fn handle_purge(&mut self, purge: &Purge) {
        if purge.relation().source() == Source::Edb {
            self.edb = self.edb.update(
                (purge.relation().id(), purge.relation().version()),
                ER::default(),
            )
        } else {
            self.idb = self.idb.update(
                (purge.relation().id(), purge.relation().version()),
                IR::default(),
            )
        }
    }

    fn handle_exit(&mut self, exit: &Exit) {
        let is_done = exit.relations().iter().all(|r| match r.source() {
            Source::Edb => self
                .edb
                .get(&(r.id(), r.version()))
                .map_or(false, ER::is_empty),

            Source::Idb => self
                .idb
                .get(&(r.id(), r.version()))
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
            assert!(relation_ref.source() == Source::Idb);

            if let Some(relation) = self.idb.get(&(relation_ref.id(), relation_ref.version())) {
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
            Term::Agg(col, col_binding) => bindings
                .get(&BindingKey::Agg(*col_binding, *col))
                .map(Arc::clone),
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
                assert!(not_in.relation().source() == Source::Idb);

                // TODO: Dry up constructing a fact from BTreeMap<ColId, Term>
                let mut bound: Vec<(ColId, Val)> = Vec::default();

                for (id, term) in not_in.cols() {
                    if let Some(val) = Self::resolve_term(term, blockstore, bindings) {
                        bound.push((*id, <Val>::clone(&val)));
                    }
                }

                let bound_fact = IF::new(not_in.relation().id(), bound);

                !self
                    .idb
                    .get(&(not_in.relation().id(), not_in.relation().version()))
                    .map(|r| r.contains(&bound_fact))
                    .unwrap()
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

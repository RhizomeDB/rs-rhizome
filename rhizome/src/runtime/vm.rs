use core::fmt::Debug;
use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;

use crate::{
    fact::{
        traits::{EDBFact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    ram::{
        operation::{project::Project, search::Search, Operation},
        program::Program,
        statement::{
            exit::Exit, insert::Insert, merge::Merge, purge::Purge, recursive::Loop, sinks::Sinks,
            sources::Sources, swap::Swap, Statement,
        },
        Bindings, Reduce,
    },
    relation::{DefaultRelation, Relation},
    storage::blockstore::Blockstore,
    timestamp::{DefaultTimestamp, Timestamp},
};

pub(crate) struct VM<
    T = DefaultTimestamp,
    EF = DefaultEDBFact,
    IF = DefaultIDBFact,
    ER = DefaultRelation<EF>,
    IR = DefaultRelation<IF>,
> where
    EF: EDBFact,
    IF: IDBFact,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
{
    timestamp: T,
    pc: (usize, Option<usize>),
    input: VecDeque<EF>,
    output: VecDeque<IF>,
    program: Program<EF, IF, ER, IR>,
}

impl<T, EF, IF, ER, IR> Debug for VM<T, EF, IF, ER, IR>
where
    T: Timestamp,
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
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
    ER: Relation<Fact = EF>,
    IR: Relation<Fact = IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    pub(crate) fn new(program: Program<EF, IF, ER, IR>) -> Self {
        Self {
            timestamp: T::default(),
            pc: (0, None),
            input: VecDeque::default(),
            output: VecDeque::default(),
            program,
        }
    }

    pub(crate) fn timestamp(&self) -> &T {
        &self.timestamp
    }

    pub(crate) fn push(&mut self, fact: EF) -> Result<()> {
        self.input.push_back(fact);

        Ok(())
    }

    pub(crate) fn pop(&mut self) -> Result<Option<IF>> {
        let fact = self.output.pop_front();

        Ok(fact)
    }

    pub(crate) fn step_epoch<BS>(&mut self, blockstore: &BS) -> Result<()>
    where
        BS: Blockstore,
    {
        assert!(self.timestamp == self.timestamp.epoch_start());

        let start = self.timestamp;

        loop {
            if !self.step(blockstore)? || self.timestamp.epoch() != start.epoch() {
                break;
            };
        }

        Ok(())
    }

    fn step<BS>(&mut self, blockstore: &BS) -> Result<bool>
    where
        BS: Blockstore,
    {
        let continue_epoch = match &*self.load_statement() {
            Statement::Insert(insert) => self.handle_insert(insert, blockstore),
            Statement::Merge(merge) => self.handle_merge(merge),
            Statement::Swap(swap) => self.handle_swap(swap),
            Statement::Purge(purge) => self.handle_purge(purge),
            Statement::Exit(exit) => {
                assert!(self.pc.1.is_some());

                self.handle_exit(exit)
            }
            Statement::Sources(sources) => self.handle_sources(sources),
            Statement::Sinks(sinks) => self.handle_sinks(sinks),
            Statement::Loop(Loop { .. }) => {
                unreachable!("load_statement follows loops in the root block")
            }
        }?;

        if !continue_epoch {
            return Ok(false);
        }

        self.pc = self.step_pc();

        if self.pc.0 == 0 {
            self.timestamp = self.timestamp.advance_epoch();
        } else if self.pc.1 == Some(0) {
            self.timestamp = self.timestamp.advance_iteration();
        };

        Ok(true)
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

    fn load_statement(&self) -> Arc<Statement<EF, IF, ER, IR>> {
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

    fn handle_insert<BS>(
        &mut self,
        insert: &Insert<EF, IF, ER, IR>,
        blockstore: &BS,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        // Only insert ground facts on the first clock cycle
        if insert.is_ground() && *self.timestamp() != self.timestamp().clock_start() {
            Ok(true)
        } else {
            self.handle_operation(insert.operation(), blockstore)
        }
    }

    fn handle_operation<BS>(
        &mut self,
        operation: &Operation<EF, IF, ER, IR>,
        blockstore: &BS,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        let bindings = Bindings::default();

        self.do_handle_operation(operation, blockstore, &bindings)
    }

    fn do_handle_operation<BS>(
        &self,
        operation: &Operation<EF, IF, ER, IR>,
        blockstore: &BS,
        bindings: &Bindings,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        match operation {
            Operation::Search(inner) => self.handle_search(inner, blockstore, bindings),
            Operation::Project(inner) => self.handle_project(inner, blockstore, bindings),
            Operation::Reduce(inner) => self.handle_reduce(inner, blockstore, bindings),
        }?;

        Ok(true)
    }

    fn handle_search<BS>(
        &self,
        search: &Search<EF, IF, ER, IR>,
        blockstore: &BS,
        bindings: &Bindings,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        search.apply(blockstore, bindings, |next_bindings| {
            self.do_handle_operation(search.operation(), blockstore, &next_bindings)
        })
    }

    fn handle_project<BS>(
        &self,
        project: &Project<EF, IF, IR>,
        blockstore: &BS,
        bindings: &Bindings,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        project.apply(blockstore, bindings);

        Ok(true)
    }

    fn handle_reduce<BS>(
        &self,
        agg: &Reduce<EF, IF, ER, IR>,
        blockstore: &BS,
        bindings: &Bindings,
    ) -> Result<bool>
    where
        BS: Blockstore,
    {
        if let Some(next_bindings) = agg.apply(blockstore, bindings) {
            self.do_handle_operation(agg.operation(), blockstore, &next_bindings)?;
        }

        Ok(true)
    }

    fn handle_merge(&self, merge: &Merge<IF, IR>) -> Result<bool> {
        merge.apply();

        Ok(true)
    }

    fn handle_swap(&self, swap: &Swap<IR>) -> Result<bool> {
        swap.apply();

        Ok(true)
    }

    fn handle_purge(&self, purge: &Purge<EF, IF, ER, IR>) -> Result<bool> {
        purge.apply();

        Ok(true)
    }

    fn handle_exit(&mut self, exit: &Exit<IF, IR>) -> Result<bool> {
        if exit.apply() {
            self.pc.1 = None;
        }

        Ok(true)
    }

    fn handle_sources(&mut self, sources: &Sources<EF, ER>) -> Result<bool> {
        Ok(sources.apply(&mut self.input)
            || self.timestamp().epoch_start() == self.timestamp().clock_start())
    }

    fn handle_sinks(&mut self, sinks: &Sinks<IF, IR>) -> Result<bool> {
        sinks.apply(&mut self.output);

        Ok(true)
    }
}

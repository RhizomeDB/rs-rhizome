use anyhow::Result;
use rhizome_runtime::Runtime;
use std::{fmt::Debug, marker::PhantomData};

use futures::{
    channel::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    select, SinkExt, StreamExt,
};

use crate::{
    build,
    fact::{
        traits::{EDBFact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    id::RelationId,
    logic::ProgramBuilder,
    relation::{DefaultRelation, Relation},
    storage::{blockstore::Blockstore, memory::MemoryBlockstore, DefaultCodec, DEFAULT_MULTIHASH},
    timestamp::{DefaultTimestamp, Timestamp},
};

use super::{vm::VM, ClientCommand, ClientEvent, SinkCommand, StreamEvent};

pub struct Reactor<
    T = DefaultTimestamp,
    BS = MemoryBlockstore,
    E = DefaultEDBFact,
    I = DefaultIDBFact,
    ER = DefaultRelation<E>,
    IR = DefaultRelation<I>,
> where
    T: Timestamp,
    E: EDBFact,
    I: IDBFact,
    ER: Relation<Fact = E>,
    IR: Relation<Fact = I>,
{
    runtime: Runtime,
    blockstore: BS,
    // TODO: set of stream IDs?
    sinks: Vec<(RelationId, mpsc::Sender<SinkCommand<I>>)>,
    // TODO: refactor events / commands to swap their names and to move processing of commands into
    // the event loop
    command_rx: mpsc::Receiver<ClientCommand<E, I>>,
    event_tx: mpsc::Sender<ClientEvent<T>>,
    stream_rx: mpsc::Receiver<StreamEvent<E>>,
    stream_tx: mpsc::Sender<StreamEvent<E>>,
    _marker: PhantomData<(ER, IR)>,
}

impl<T, BS, E, I, ER, IR> Reactor<T, BS, E, I, ER, IR>
where
    T: Timestamp,
    BS: Blockstore,
    E: EDBFact + 'static,
    I: IDBFact + 'static,
    ER: Relation<Fact = E>,
    IR: Relation<Fact = I>,
{
    pub fn new(
        command_rx: Receiver<ClientCommand<E, I>>,
        event_tx: Sender<ClientEvent<T>>,
    ) -> Self
where {
        let (stream_tx, stream_rx) = mpsc::channel(10);

        Self {
            runtime: Runtime::default(),
            blockstore: BS::default(),
            sinks: Vec::default(),
            command_rx,
            event_tx,
            stream_tx,
            stream_rx,
            _marker: PhantomData::default(),
        }
    }

    pub async fn async_run<F>(mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut ProgramBuilder) -> Result<()>,
    {
        let program = build(f)?;
        let mut vm = VM::<T, E, I, ER, IR>::new(program);

        loop {
            select! {
                command = self.command_rx.next() => if let Some(c) = command {
                    self.handle_command(&mut vm, c).await.unwrap();
                },
                event = self.stream_rx.next() => if let Some(e) = event {
                    self.handle_event(&mut vm, e).await.unwrap();
                }
            }

            // TODO: use a buffered blockstore and flush after each iteration?
            vm.step_epoch(&self.blockstore)?;

            while let Ok(Some(fact)) = vm.pop() {
                // TODO: index sinks by relation_id
                for (relation_id, sink) in &self.sinks {
                    if fact.id() == *relation_id {
                        sink.clone()
                            .send(SinkCommand::ProcessFact(fact.clone()))
                            .await?;
                    }
                }
            }

            self.event_tx
                .send(ClientEvent::ReachedFixedpoint(*vm.timestamp()))
                .await?;
        }
    }

    async fn handle_command(
        &mut self,
        vm: &mut VM<T, E, I, ER, IR>,
        command: ClientCommand<E, I>,
    ) -> Result<()> {
        match command {
            ClientCommand::Flush(sender) => {
                let mut handles = Vec::default();

                for (_, sink) in &self.sinks {
                    let (tx, rx) = oneshot::channel();
                    sink.clone().send(SinkCommand::Flush(tx)).await?;

                    handles.push(rx);
                }

                for handle in handles {
                    handle.await?;
                }

                sender.send(()).unwrap();
            }
            ClientCommand::InsertFact(fact, sender) => {
                self.blockstore.put_serializable(
                    &fact,
                    DefaultCodec::default(),
                    DEFAULT_MULTIHASH,
                )?;

                vm.push(fact)?;

                sender.send(()).unwrap();
            }
            ClientCommand::RegisterStream(_, create_stream, sender) => {
                let mut tx = self.stream_tx.clone();
                let create_task = move || async move {
                    let mut stream = Box::into_pin(create_stream());

                    while let Some(fact) = stream.next().await {
                        tx.send(StreamEvent::Fact(fact)).await.unwrap();
                    }
                };

                self.runtime.spawn_pinned(create_task);

                sender.send(()).unwrap();
            }
            ClientCommand::RegisterSink(id, create_sink, sender) => {
                let (tx, mut rx) = mpsc::channel(1);
                let create_task = move || async move {
                    let mut sink = Box::into_pin(create_sink());

                    loop {
                        match rx.next().await {
                            Some(SinkCommand::Flush(sender)) => sender.send(()).unwrap(),
                            Some(SinkCommand::ProcessFact(fact)) => sink.send(fact).await.unwrap(),
                            None => break,
                        };
                    }
                };

                self.runtime.spawn_pinned(create_task);
                self.sinks.push((id, tx));

                sender.send(()).unwrap();
            }
        };

        Ok(())
    }

    async fn handle_event(
        &mut self,
        vm: &mut VM<T, E, I, ER, IR>,
        event: StreamEvent<E>,
    ) -> Result<()> {
        match event {
            StreamEvent::Fact(fact) => {
                self.blockstore.put_serializable(
                    &fact,
                    DefaultCodec::default(),
                    DEFAULT_MULTIHASH,
                )?;

                vm.push(fact)?;
            }
        };

        Ok(())
    }
}

impl<T, BS, E, I> Debug for Reactor<T, BS, E, I>
where
    T: Timestamp,
    E: EDBFact + 'static,
    I: IDBFact + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish()
    }
}

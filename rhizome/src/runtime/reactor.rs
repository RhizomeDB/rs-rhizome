use anyhow::Result;
use cid::Cid;
use rhizome_runtime::Runtime;
use std::{collections::HashMap, fmt::Debug};

use futures::{
    channel::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    select, SinkExt, StreamExt,
};

use crate::{
    build,
    error::Error,
    id::RelationId,
    logic::ProgramBuilder,
    storage::{
        buffered::{Buffered, BufferedBlockstore},
        codec::DagCbor,
        content_addressable::ContentAddressable,
        memory::MemoryBlockstore,
        DefaultCodec, DEFAULT_MULTIHASH,
    },
    timestamp::{DefaultTimestamp, Timestamp},
    tuple::InputTuple,
};

use super::{epoch::Epoch, vm::VM, ClientCommand, ClientEvent, SinkCommand, StreamEvent};

pub struct Reactor<T = DefaultTimestamp, BS = BufferedBlockstore<MemoryBlockstore>>
where
    T: Timestamp,
{
    runtime: Runtime,
    blockstore: BS,
    staging_epoch: Epoch,
    active_epoch: Epoch,
    epoch_stack: Vec<Cid>,
    sinks: HashMap<RelationId, Vec<mpsc::Sender<SinkCommand>>>,
    command_rx: mpsc::Receiver<ClientCommand>,
    event_tx: mpsc::Sender<ClientEvent<T>>,
    stream_rx: mpsc::Receiver<StreamEvent>,
    stream_tx: mpsc::Sender<StreamEvent>,
}

impl<T, BS> Reactor<T, BS>
where
    T: Timestamp,
    BS: Buffered + Default,
{
    pub fn new(command_rx: Receiver<ClientCommand>, event_tx: Sender<ClientEvent<T>>) -> Self
where {
        let (stream_tx, stream_rx) = mpsc::channel(10);

        let active_epoch = Epoch::default();

        Self {
            runtime: Default::default(),
            blockstore: Default::default(),
            staging_epoch: active_epoch.step_epoch().unwrap(),
            active_epoch,
            epoch_stack: Default::default(),
            sinks: Default::default(),
            command_rx,
            event_tx,
            stream_tx,
            stream_rx,
        }
    }

    pub async fn async_run<F>(mut self, f: F) -> Result<()>
    where
        F: FnOnce(ProgramBuilder) -> Result<ProgramBuilder>,
    {
        let program = build(f)?;
        let is_monotonic = program.is_monotonic();
        let mut vm = VM::<T>::new(program);

        loop {
            // Poll for any future and then run all ready futures
            select! {
                command = self.command_rx.next() => if let Some(c) = command {
                    self.handle_command(c).await?;
                },
                event = self.stream_rx.next() => if let Some(e) = event {
                    self.handle_event(e).await?;
                },
            }

            loop {
                select! {
                    command = self.command_rx.next() => if let Some(c) = command {
                        self.handle_command(c).await?;
                    },
                    event = self.stream_rx.next() => if let Some(e) = event {
                        self.handle_event(e).await?;
                    },
                    default => break
                }
            }

            // We are at the head epoch, so we can simply step the epoch as normal
            if self.epoch_stack.is_empty() {
                // If there are no tuples pending, then continue to the next iteration of the loop
                // without stepping to the next epoch, to avoid creating an empty epoch for monotonic
                // programs, and to avoid performing unnecessary work for non-monotonic programs.
                if !self.staging_epoch.has_tuples_pending() {
                    continue;
                }

                self.active_epoch = self.staging_epoch;
                self.staging_epoch = self.active_epoch.step_epoch()?;

                self.blockstore.put_serializable(
                    &self.active_epoch,
                    DefaultCodec::default(),
                    DEFAULT_MULTIHASH,
                )?;

                self.blockstore.flush(&self.active_epoch.cid()?)?;

                if is_monotonic {
                    self.active_epoch.with_tuples(
                        &self.blockstore,
                        &mut |input_tuple: InputTuple| {
                            for tuple in input_tuple.normalize_as_tuples()? {
                                vm.push(tuple)?;
                            }

                            Ok(())
                        },
                    )?;
                } else {
                    vm.reset_relations()?;

                    self.active_epoch.with_tuples_rec(
                        &self.blockstore,
                        &mut |input_tuple: InputTuple| {
                            for tuple in input_tuple.normalize_as_tuples()? {
                                vm.push(tuple)?;
                            }

                            Ok(())
                        },
                    )?;
                }
            } else {
                // We are rewound to a previous epoch, so we need to reset the relations,
                // and load the tuples observed as of that epoch.
                vm.reset_relations()?;

                self.active_epoch.with_tuples_rec(
                    &self.blockstore,
                    &mut |input_tuple: InputTuple| {
                        for tuple in input_tuple.normalize_as_tuples()? {
                            vm.push(tuple)?;
                        }

                        Ok(())
                    },
                )?;
            }

            // TODO: The VM currently tracks its own timestamp, but perhaps that should be
            // moved into the epoch itself, so that we don't need to worry about the timestamp
            // of the VM falling out of sync with the timetamp of the reactor. Then a cleaner
            // interface might be to expose VM::compute_at_epoch(epoch), which can handle all of
            // the above setup.
            vm.step_epoch(&self.blockstore)?;

            while let Ok(Some(tuple)) = vm.pop() {
                if let Some(sinks) = self.sinks.get_mut(&tuple.id()) {
                    for sink in sinks {
                        sink.send(SinkCommand::ProcessTuple(tuple.clone())).await?;
                    }
                }
            }

            self.event_tx
                .send(ClientEvent::ReachedFixedpoint(
                    *vm.timestamp(),
                    self.active_epoch.cid()?,
                ))
                .await?;
        }
    }

    async fn handle_command(&mut self, command: ClientCommand) -> Result<()> {
        match command {
            ClientCommand::Flush(sender) => {
                let mut handles = Vec::default();

                for sinks in self.sinks.values_mut() {
                    for sink in sinks.iter_mut() {
                        let (tx, rx) = oneshot::channel();
                        sink.send(SinkCommand::Flush(tx)).await?;

                        handles.push(rx);
                    }
                }

                for handle in handles {
                    handle.await?;
                }

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
            ClientCommand::InsertTuple(tuple, sender) => {
                self.blockstore.put_serializable(
                    &tuple,
                    #[allow(unknown_lints, clippy::default_constructed_unit_structs)]
                    DefaultCodec::default(),
                    DEFAULT_MULTIHASH,
                )?;

                self.staging_epoch.push_tuple(*tuple)?;

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
            ClientCommand::RegisterStream(_, create_stream, sender) => {
                let mut tx = self.stream_tx.clone();
                let create_task = move || async move {
                    let mut stream = Box::into_pin(create_stream());

                    while let Some(tuple) = stream.next().await {
                        tx.send(StreamEvent::Tuple(tuple))
                            .await
                            .expect("stream channel closed");
                    }
                };

                self.runtime.spawn_pinned(create_task);

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
            ClientCommand::RegisterSink(id, create_sink, sender) => {
                let (tx, mut rx) = mpsc::channel(100);
                let create_task = move || async move {
                    let mut sink = Box::into_pin(create_sink());

                    loop {
                        match rx.next().await {
                            Some(SinkCommand::Flush(sender)) => {
                                sender.send(()).expect("reactor channel closed")
                            }
                            Some(SinkCommand::ProcessTuple(tuple)) => {
                                sink.send(tuple).await.expect("reactor channel closed")
                            }
                            None => break,
                        };
                    }
                };

                self.runtime.spawn_pinned(create_task);
                self.sinks.entry(id).or_default().push(tx);

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
            ClientCommand::RewindEpoch(sender) => {
                if let Some(epoch) = self.active_epoch.rewind(&self.blockstore)? {
                    self.epoch_stack.push(self.active_epoch.cid()?);
                    self.active_epoch = epoch;
                }

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
            ClientCommand::ReplayEpoch(sender) => {
                if let Some(cid) = self.epoch_stack.pop() {
                    self.active_epoch = self
                        .blockstore
                        .get_serializable::<DagCbor, Epoch>(&cid)?
                        .unwrap();
                }

                sender
                    .send(())
                    .map_err(|_| Error::InternalRhizomeError("client channel closed".to_owned()))?;
            }
        };

        Ok(())
    }

    async fn handle_event(&mut self, event: StreamEvent) -> Result<()> {
        match event {
            StreamEvent::Tuple(tuple) => {
                self.blockstore.put_serializable(
                    &tuple,
                    #[allow(unknown_lints, clippy::default_constructed_unit_structs)]
                    DefaultCodec::default(),
                    DEFAULT_MULTIHASH,
                )?;

                self.staging_epoch.push_tuple(tuple)?;
            }
        };

        Ok(())
    }
}

impl<T, BS> Debug for Reactor<T, BS>
where
    T: Timestamp,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish()
    }
}

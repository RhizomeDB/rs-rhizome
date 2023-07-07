use anyhow::Result;
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
        content_addressable::ContentAddressable,
        memory::MemoryBlockstore,
        DefaultCodec, DEFAULT_MULTIHASH,
    },
    timestamp::{DefaultTimestamp, Timestamp},
    tuple::Tuple,
};

use super::{epoch::Epoch, vm::VM, ClientCommand, ClientEvent, SinkCommand, StreamEvent};

pub struct Reactor<T = DefaultTimestamp, BS = BufferedBlockstore<MemoryBlockstore>>
where
    T: Timestamp,
{
    runtime: Runtime,
    blockstore: BS,
    epoch: Epoch,
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

        Self {
            runtime: Default::default(),
            blockstore: Default::default(),
            epoch: Default::default(),
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

            self.blockstore.flush(&self.epoch.cid()?)?;
            self.epoch
                .with_tuples(&self.blockstore, &mut |input_tuple| {
                    let cid = input_tuple.cid()?;
                    let tuple = Tuple::new(
                        "evac",
                        [
                            ("entity", input_tuple.entity()),
                            ("attribute", input_tuple.attr()),
                            ("value", input_tuple.val()),
                        ],
                        Some(cid),
                    );

                    vm.push(tuple)?;

                    for link in input_tuple.links() {
                        let tuple = Tuple::new("links", [("from", cid), ("to", *link)], None);

                        vm.push(tuple)?;
                    }

                    Ok(())
                })?;

            vm.step_epoch(&self.blockstore)?;

            self.epoch = self.epoch.step_epoch()?;

            while let Ok(Some(tuple)) = vm.pop() {
                if let Some(sinks) = self.sinks.get_mut(&tuple.id()) {
                    for sink in sinks {
                        sink.send(SinkCommand::ProcessTuple(tuple.clone())).await?;
                    }
                }
            }

            self.event_tx
                .send(ClientEvent::ReachedFixedpoint(*vm.timestamp()))
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

                self.epoch.push_tuple(*tuple)?;

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

                self.epoch.push_tuple(tuple)?;
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

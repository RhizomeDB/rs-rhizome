use anyhow::Result;
use std::{fmt::Debug, marker::PhantomData};

use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

use crate::{
    fact::{DefaultEDBFact, DefaultIDBFact},
    id::RelationId,
    ram::Program,
    timestamp::DefaultTimestamp,
};

use super::{reactor::Reactor, vm::VM, ClientCommand, ClientEvent, CreateSink, CreateStream};

#[derive(Debug)]
pub struct Client {
    command_tx: mpsc::Sender<ClientCommand<DefaultEDBFact, DefaultIDBFact>>,
    _marker: PhantomData<DefaultIDBFact>,
}

impl Client {
    pub fn new(program: Program) -> (Self, mpsc::Receiver<ClientEvent<DefaultTimestamp>>, Reactor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(1);

        let client = Self {
            command_tx,
            _marker: PhantomData::default(),
        };

        // Unstable type parameter fallback means that the ergonomics of default type
        // parameters aren't great, so for now we just use the defaults directly, without
        // any type inference. We can improve this experience using GATs.
        let vm = <VM>::new(program);
        let reactor = <Reactor>::new(vm, command_rx, event_tx);

        (client, event_rx, reactor)
    }

    pub async fn flush(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.command_tx.send(ClientCommand::Flush(tx)).await?;

        rx.await?;

        Ok(())
    }

    pub async fn insert_fact(&mut self, fact: DefaultEDBFact) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::InsertFact(fact, tx))
            .await?;

        rx.await?;

        Ok(())
    }

    pub async fn register_stream<F>(&mut self, id: &str, f: F) -> Result<()>
    where
        F: CreateStream<DefaultEDBFact> + 'static,
    {
        let id = RelationId::new(id);
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::RegisterStream(id, Box::new(f), tx))
            .await?;

        rx.await?;

        Ok(())
    }

    pub async fn register_sink(
        &mut self,
        id: &str,
        f: Box<dyn CreateSink<DefaultIDBFact>>,
    ) -> Result<()> {
        let id = RelationId::new(id);
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::RegisterSink(id, f, tx))
            .await?;

        rx.await?;

        Ok(())
    }
}

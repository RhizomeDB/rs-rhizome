use anyhow::Result;
use std::{fmt::Debug, marker::PhantomData};

use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

use crate::{
    fact::{DefaultEDBFact, DefaultIDBFact},
    id::RelationId,
    timestamp::DefaultTimestamp,
};

use super::{reactor::Reactor, ClientCommand, ClientEvent, CreateSink, CreateStream};

#[derive(Debug)]
pub struct Client {
    command_tx: mpsc::Sender<ClientCommand<DefaultEDBFact, DefaultIDBFact>>,
    _marker: PhantomData<DefaultIDBFact>,
}

impl Client {
    pub fn new() -> (Self, mpsc::Receiver<ClientEvent<DefaultTimestamp>>, Reactor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(1);

        let client = Self {
            command_tx,
            _marker: PhantomData::default(),
        };

        let reactor = <Reactor>::new(command_rx, event_tx);

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

    pub async fn register_stream(
        &mut self,
        id: &str,
        f: Box<dyn CreateStream<DefaultEDBFact>>,
    ) -> Result<()> {
        let id = RelationId::new(id);
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::RegisterStream(id, f, tx))
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

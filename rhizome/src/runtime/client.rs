use anyhow::Result;
use std::fmt::Debug;

use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

use crate::{id::RelationId, timestamp::DefaultTimestamp, tuple::InputTuple};

use super::{reactor::Reactor, ClientCommand, ClientEvent, CreateSink, CreateStream};

#[derive(Debug)]
pub struct Client {
    command_tx: mpsc::Sender<ClientCommand>,
}

impl Client {
    pub fn new() -> (Self, mpsc::Receiver<ClientEvent<DefaultTimestamp>>, Reactor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(1);

        let client = Self { command_tx };

        let reactor = <Reactor>::new(command_rx, event_tx);

        (client, event_rx, reactor)
    }

    pub async fn flush(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.command_tx.send(ClientCommand::Flush(tx)).await?;

        rx.await?;

        Ok(())
    }

    pub async fn insert_tuple(&mut self, tuple: InputTuple) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::InsertTuple(Box::new(tuple), tx))
            .await?;

        rx.await?;

        Ok(())
    }

    pub async fn register_stream(&mut self, id: &str, f: Box<dyn CreateStream>) -> Result<()> {
        let id = RelationId::new(id);
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::RegisterStream(id, f, tx))
            .await?;

        rx.await?;

        Ok(())
    }

    pub async fn register_sink(&mut self, id: &str, f: Box<dyn CreateSink>) -> Result<()> {
        let id = RelationId::new(id);
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::RegisterSink(id, f, tx))
            .await?;

        rx.await?;

        Ok(())
    }
}

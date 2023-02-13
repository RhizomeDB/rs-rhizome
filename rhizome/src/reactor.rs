use anyhow::Result;
use rhizome_runtime::{MaybeSend, Runtime};
use std::fmt::Debug;

use futures::{
    channel::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    sink::unfold,
    Sink, SinkExt, Stream, StreamExt,
};

use crate::{
    error::Error,
    fact::Fact,
    id::RelationId,
    ram::vm::VM,
    relation::{DefaultRelation, Relation},
    timestamp::{DefaultTimestamp, Timestamp},
};

pub type FactStream = Box<dyn Stream<Item = Fact>>;
pub type FactSink = Box<dyn Sink<Fact, Error = Error>>;

#[derive(Debug)]
pub enum SinkMsg<T> {
    Payload(T),
    Flush(oneshot::Sender<()>),
}

pub enum Event {
    // TODO: stream ID?
    RegisteredStream,
    RegisteredSink(RelationId, UnboundedSender<SinkMsg<Fact>>),
    // TODO: stream ID?
    StreamClosed,
    // TODO: Associate incoming facts with their stream?
    FactInserted(Fact),
}

impl Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::RegisteredStream => f.debug_struct("RegisteredStream").finish(),
            Event::RegisteredSink(relation_id, _) => f
                .debug_struct("RegisteredSink")
                .field("RelationId", relation_id)
                .finish(),
            Event::StreamClosed => f.debug_struct("StreamClosed").finish(),
            Event::FactInserted(fact) => {
                f.debug_struct("FactInserted").field("Fact", fact).finish()
            }
        }
    }
}

pub struct Reactor<R = DefaultRelation, T = DefaultTimestamp>
where
    R: Relation,
    T: Timestamp,
{
    runtime: Runtime,
    vm: VM<T, R>,
    // TODO: set of stream IDs?
    num_streams: usize,
    sinks: Vec<(RelationId, mpsc::UnboundedSender<SinkMsg<Fact>>)>,
    // TODO: use a bounded channel?
    events_rx: mpsc::UnboundedReceiver<Event>,
    events_tx: mpsc::UnboundedSender<Event>,
    subscribers: Vec<oneshot::Sender<()>>,
}

impl<R, T> Reactor<R, T>
where
    T: Timestamp,
    R: Relation,
{
    pub fn new(vm: VM<T, R>) -> Self {
        let (tx, rx) = mpsc::unbounded();

        Self {
            runtime: Runtime::default(),
            vm,
            num_streams: 0,
            sinks: Vec::default(),
            events_rx: rx,
            events_tx: tx,
            subscribers: Vec::default(),
        }
    }

    pub fn subscribe(&mut self, tx: oneshot::Sender<()>) {
        self.subscribers.push(tx);
    }

    pub fn input_channel(&self) -> Result<mpsc::UnboundedSender<Fact>> {
        let (tx, rx) = mpsc::unbounded();

        self.register_stream(|| {
            Box::new(futures::stream::unfold(rx, move |mut rx| async move {
                rx.next().await.map(|fact| (fact, rx))
            }))
        })?;

        Ok(tx)
    }

    pub fn output_channel(&self, relation_id: RelationId) -> Result<mpsc::UnboundedReceiver<Fact>> {
        let (tx, rx) = mpsc::unbounded();

        self.register_sink(relation_id, || {
            Box::new(unfold(tx, move |tx, fact: Fact| async move {
                tx.unbounded_send(fact).expect("channel disconnected");

                Ok(tx)
            }))
        })?;

        Ok(rx)
    }

    pub fn register_stream<F>(&self, create_stream: F) -> Result<()>
    where
        F: (FnOnce() -> FactStream),
        F: MaybeSend + 'static,
    {
        let tx = self.events_tx.clone();
        let create_task = move || async move {
            let mut stream = Box::into_pin(create_stream());

            while let Some(fact) = stream.next().await {
                tx.unbounded_send(Event::FactInserted(fact))
                    .expect("channel disconnected")
            }

            tx.unbounded_send(Event::StreamClosed)
                .expect("channel disconnected")
        };

        self.runtime.spawn_pinned(create_task);

        self.events_tx.unbounded_send(Event::RegisteredStream)?;

        Ok(())
    }

    pub fn register_sink<F>(&self, relation_id: impl Into<RelationId>, create_sink: F) -> Result<()>
    where
        F: (FnOnce() -> FactSink),
        F: MaybeSend + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded();
        let create_task = move || async move {
            let mut sink = Box::into_pin(create_sink());

            loop {
                match rx.next().await {
                    Some(SinkMsg::Payload(fact)) => {
                        sink.send(fact).await.expect("channel disconnected")
                    }
                    Some(SinkMsg::Flush(flush_tx)) => {
                        flush_tx.send(()).expect("channel disconnected")
                    }
                    None => break,
                };
            }
        };

        self.runtime.spawn_pinned(create_task);

        self.events_tx
            .unbounded_send(Event::RegisteredSink(relation_id.into(), tx))?;

        Ok(())
    }

    pub async fn async_run(&mut self) -> Result<()> {
        loop {
            self.tick().await?;

            if self.num_streams == 0 {
                // Wait for every sink to flush
                let mut flush_handles = Vec::default();
                for (_, sink) in &self.sinks {
                    let (tx, rx) = oneshot::channel();

                    flush_handles.push(rx);

                    sink.unbounded_send(SinkMsg::Flush(tx))?;
                }

                for handle in flush_handles {
                    handle.await?;
                }

                while let Some(subscriber) = self.subscribers.pop() {
                    let _ = subscriber.send(());
                }
            }
        }
    }

    pub async fn tick(&mut self) -> Result<()> {
        let mut input_channel = self.vm.input_channel();
        let mut next_event = Ok(self.events_rx.next().await);
        while let Ok(Some(event)) = next_event {
            match event {
                Event::RegisteredStream => {
                    self.num_streams += 1;
                }
                Event::RegisteredSink(relation_id, tx) => self.sinks.push((relation_id, tx)),
                Event::StreamClosed => self.num_streams -= 1,
                Event::FactInserted(fact) => input_channel.unbounded_send(fact)?,
            }

            next_event = self.events_rx.try_next();
        }
        input_channel.close().await?;

        let mut sinks = Vec::default();
        for (relation_id, sink) in &self.sinks {
            let output_channel = self.vm.output_channel(*relation_id);

            sinks.push((output_channel, sink));
        }

        self.vm.step_epoch()?;

        while let Some((mut output_channel, sink)) = sinks.pop() {
            while let Ok(Some(fact)) = output_channel.try_next() {
                sink.unbounded_send(SinkMsg::Payload(fact))?;
            }

            output_channel.close();
        }

        Ok(())
    }
}

impl<R, T> Debug for Reactor<R, T>
where
    R: Relation,
    T: Timestamp,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish()
    }
}

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
    fact::{
        traits::{EDBFact, IDBFact},
        DefaultEDBFact, DefaultIDBFact,
    },
    id::RelationId,
    ram::vm::VM,
    relation::{DefaultRelation, Relation},
    storage::{blockstore::Blockstore, memory::MemoryBlockstore, DefaultCodec, DEFAULT_MULTIHASH},
    timestamp::{DefaultTimestamp, Timestamp},
};

pub type FactStream<F> = Box<dyn Stream<Item = F>>;
pub type FactSink<F> = Box<dyn Sink<F, Error = Error>>;

#[derive(Debug)]
pub enum SinkMsg<F> {
    Payload(F),
    Flush(oneshot::Sender<()>),
}

pub enum Event<EF, IF> {
    // TODO: stream ID?
    RegisteredStream,
    RegisteredSink(RelationId, UnboundedSender<SinkMsg<IF>>),
    // TODO: stream ID?
    StreamClosed,
    // TODO: Associate incoming facts with their stream?
    FactInserted(EF),
}

impl<EF, IF> Debug for Event<EF, IF>
where
    EF: IDBFact + 'static,
{
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

pub struct Reactor<
    T = DefaultTimestamp,
    BS = MemoryBlockstore,
    EF = DefaultEDBFact,
    IF = DefaultIDBFact,
    ER = DefaultRelation<EF>,
    IR = DefaultRelation<IF>,
> {
    runtime: Runtime,
    vm: VM<T, EF, IF, ER, IR>,
    blockstore: BS,
    // TODO: set of stream IDs?
    num_streams: usize,
    sinks: Vec<(RelationId, mpsc::UnboundedSender<SinkMsg<IF>>)>,
    // TODO: use a bounded channel?
    events_rx: mpsc::UnboundedReceiver<Event<EF, IF>>,
    events_tx: mpsc::UnboundedSender<Event<EF, IF>>,
    subscribers: Vec<oneshot::Sender<()>>,
}

impl<T, BS, EF, IF, ER, IR> Reactor<T, BS, EF, IF, ER, IR>
where
    T: Timestamp,
    BS: Blockstore,
    EF: EDBFact + 'static,
    IF: IDBFact + 'static,
    ER: Relation<EF>,
    IR: Relation<IF>,
{
    pub fn new(vm: VM<T, EF, IF, ER, IR>) -> Self {
        let (tx, rx) = mpsc::unbounded();

        Self {
            runtime: Runtime::default(),
            vm,
            blockstore: BS::default(),
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

    pub fn input_channel(&self) -> Result<mpsc::UnboundedSender<EF>> {
        let (tx, rx) = mpsc::unbounded();

        self.register_stream(|| {
            Box::new(futures::stream::unfold(rx, move |mut rx| async move {
                rx.next().await.map(|fact| (fact, rx))
            }))
        })?;

        Ok(tx)
    }

    pub fn output_channel(&self, relation_id: RelationId) -> Result<mpsc::UnboundedReceiver<IF>> {
        let (tx, rx) = mpsc::unbounded();

        self.register_sink(relation_id, || {
            Box::new(unfold(tx, move |tx, fact: IF| async move {
                tx.unbounded_send(fact).expect("channel disconnected");

                Ok(tx)
            }))
        })?;

        Ok(rx)
    }

    pub fn register_stream<F>(&self, create_stream: F) -> Result<()>
    where
        F: (FnOnce() -> FactStream<EF>),
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
        F: (FnOnce() -> FactSink<IF>),
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
        let mut next_event = Ok(self.events_rx.next().await);
        while let Ok(Some(event)) = next_event {
            match event {
                Event::RegisteredStream => {
                    self.num_streams += 1;
                }
                Event::RegisteredSink(relation_id, tx) => self.sinks.push((relation_id, tx)),
                Event::StreamClosed => self.num_streams -= 1,
                Event::FactInserted(fact) => {
                    self.blockstore
                        .put_serializable(&fact, DefaultCodec::default(), DEFAULT_MULTIHASH)
                        .unwrap();

                    println!("{}", fact);

                    self.vm.push(fact)?
                }
            }

            next_event = self.events_rx.try_next();
        }

        // TODO: use a buffered blockstore and flush after each iteration?
        self.vm.step_epoch(&self.blockstore)?;

        while let Ok(Some(fact)) = self.vm.pop() {
            // TODO: index sinks by relation_id
            for (relation_id, sink) in &self.sinks {
                if fact.id() == *relation_id {
                    sink.unbounded_send(SinkMsg::Payload(fact.clone()))?;
                }
            }
        }

        Ok(())
    }
}

impl<T, BS, EF, IF, ER, IR> Debug for Reactor<T, BS, EF, IF, ER, IR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish()
    }
}

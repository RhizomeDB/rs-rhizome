use std::{
    cell::RefCell,
    io,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use futures::{
    channel::mpsc::{unbounded, UnboundedSender},
    stream::StreamExt,
    Future,
};
use once_cell::sync::Lazy;
use tokio::task::{spawn_local, LocalSet};

type SpawnTask = Box<dyn Send + FnOnce()>;

static DEFAULT_WORKER_NAME: &str = "rhizomedb-runtime-worker";

thread_local! {
    static TASK_COUNT: RefCell<Option<Arc<AtomicUsize>>> = RefCell::new(None);
    static LOCAL_SET: LocalSet = LocalSet::new()
}

#[derive(Clone)]
pub struct LocalWorker {
    task_count: Arc<AtomicUsize>,
    tx: UnboundedSender<SpawnTask>,
}

impl LocalWorker {
    pub fn new() -> io::Result<Self> {
        let (tx, mut rx) = unbounded::<SpawnTask>();
        let task_count: Arc<AtomicUsize> = Arc::default();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        {
            let task_count = task_count.clone();
            thread::Builder::new()
                .name(DEFAULT_WORKER_NAME.into())
                .spawn(move || {
                    TASK_COUNT.with(move |m| {
                        *m.borrow_mut() = Some(task_count);
                    });

                    LOCAL_SET.with(|local_set| {
                        local_set.block_on(&rt, async move {
                            while let Some(m) = rx.next().await {
                                m();
                            }
                        });
                    });
                })?;
        }

        Ok(Self { task_count, tx })
    }

    pub fn task_count(&self) -> usize {
        self.task_count.load(Ordering::Acquire)
    }

    pub fn spawn_pinned<F, Fut>(&self, f: F)
    where
        F: FnOnce() -> Fut,
        F: Send + 'static,
        Fut: 'static + Future<Output = ()>,
    {
        let guard = LocalJobCountGuard::new(self.task_count.clone());

        // We ignore the result upon a failure, this can never happen unless the runtime is
        // exiting which all instances of Runtime will be dropped at that time and hence cannot
        // spawn pinned tasks.
        let _ = self.tx.unbounded_send(Box::new(move || {
            spawn_local(async move {
                let _guard = guard;

                f().await;
            });
        }));
    }
}

pub struct LocalJobCountGuard(Arc<AtomicUsize>);

impl LocalJobCountGuard {
    fn new(inner: Arc<AtomicUsize>) -> Self {
        inner.fetch_add(1, Ordering::AcqRel);

        LocalJobCountGuard(inner)
    }
}

impl Drop for LocalJobCountGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct Runtime {
    workers: Arc<Vec<LocalWorker>>,
}

impl Runtime {
    pub fn new(num_workers: usize) -> io::Result<Self> {
        assert!(num_workers > 0, "must have more than 1 worker.");

        let mut workers = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let worker = LocalWorker::new()?;
            workers.push(worker);
        }

        Ok(Self {
            workers: workers.into(),
        })
    }

    pub fn spawn_local<F>(f: F)
    where
        F: Future<Output = ()> + 'static,
    {
        match LocalHandle::try_current() {
            Some(m) => {
                m.spawn_local(f);
            }
            None => {
                tokio::task::spawn_local(f);
            }
        }
    }

    pub fn spawn_pinned<F, Fut>(&self, create_task: F)
    where
        F: FnOnce() -> Fut,
        F: Send + 'static,
        Fut: futures::Future<Output = ()> + 'static,
    {
        let worker = self.find_least_busy_local_worker();
        worker.spawn_pinned(create_task);
    }

    fn find_least_busy_local_worker(&self) -> &LocalWorker {
        let mut workers = self.workers.iter();

        let mut worker = workers.next().expect("must have more than 1 worker.");
        let mut task_count = worker.task_count();

        for current_worker in workers {
            if task_count == 0 {
                break;
            }

            let current_worker_task_count = current_worker.task_count();

            if current_worker_task_count < task_count {
                task_count = current_worker_task_count;
                worker = current_worker;
            }
        }

        worker
    }
}

impl Default for Runtime {
    fn default() -> Self {
        static DEFAULT_RT: Lazy<Runtime> =
            Lazy::new(|| Runtime::new(num_cpus::get()).expect("failed to create runtime."));

        DEFAULT_RT.clone()
    }
}

#[derive(Debug, Clone)]
pub struct LocalHandle {
    _marker: PhantomData<*const ()>,
    task_count: Arc<AtomicUsize>,
}

impl LocalHandle {
    pub fn current() -> Self {
        Self::try_current().expect("outside of runtime.")
    }

    fn try_current() -> Option<Self> {
        // We cache the handle to prevent borrowing RefCell.
        thread_local! {
            static LOCAL_HANDLE: Option<LocalHandle> = TASK_COUNT
                .with(|m| m.borrow().clone())
                .map(|task_count| LocalHandle { task_count, _marker: PhantomData });
        }

        LOCAL_HANDLE.with(|m| m.clone())
    }

    pub fn spawn_local<F>(&self, f: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let guard = LocalJobCountGuard::new(self.task_count.clone());

        LOCAL_SET.with(move |local_set| {
            local_set.spawn_local(async move {
                let _guard = guard;

                f.await
            })
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::channel::oneshot;
    use tokio::{sync::Barrier, test, time::timeout};

    use super::*;

    #[test]
    async fn test_spawn_pinned_least_busy() {
        let runtime = Runtime::new(2).expect("failed to create runtime.");

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let barrier = Arc::new(Barrier::new(2));

        {
            let barrier = barrier.clone();
            runtime.spawn_pinned(move || async move {
                barrier.wait().await;

                tx1.send(std::thread::current().id())
                    .expect("failed to send!");
            });
        }

        runtime.spawn_pinned(move || async move {
            barrier.wait().await;

            tx2.send(std::thread::current().id())
                .expect("failed to send!");
        });

        let result1 = timeout(Duration::from_secs(5), rx1)
            .await
            .expect("task timed out.")
            .expect("failed to receive.");

        let result2 = timeout(Duration::from_secs(5), rx2)
            .await
            .expect("task timed out.")
            .expect("failed to receive.");

        // first task and second task are not on the same thread.
        assert_ne!(result1, result2);
    }

    #[test]
    async fn test_spawn_local_within_send() {
        let runtime = Runtime::default();

        let (tx, rx) = oneshot::channel();

        runtime.spawn_pinned(move || async move {
            tokio::task::spawn(async move {
                Runtime::spawn_local(async move {
                    tx.send(()).expect("failed to send!");
                })
            });
        });

        timeout(Duration::from_secs(5), rx)
            .await
            .expect("task timed out.")
            .expect("failed to receive.");
    }
}

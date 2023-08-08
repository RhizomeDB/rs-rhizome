use std::{future::Future, marker::PhantomData};

use wasm_bindgen_futures::spawn_local;

#[derive(Debug, Clone, Default)]
pub struct Runtime {}

impl Runtime {
    pub fn new() -> Self {
        Runtime {}
    }

    pub fn spawn_local<F>(f: F)
    where
        F: Future<Output = ()> + 'static,
    {
        spawn_local(f)
    }

    pub fn spawn_pinned<F, Fut>(&self, create_task: F)
    where
        F: FnOnce() -> Fut,
        F: 'static,
        Fut: Future<Output = ()> + 'static,
    {
        spawn_local(create_task())
    }
}

#[derive(Debug, Clone)]
pub struct LocalHandle {
    // This type is not send or sync.
    _marker: PhantomData<*const ()>,
}

impl LocalHandle {
    pub fn try_current() -> Option<Self> {
        Some(Self {
            _marker: PhantomData,
        })
    }

    pub fn current() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    pub fn spawn_local<F>(&self, f: F)
    where
        F: Future<Output = ()> + 'static,
    {
        spawn_local(f);
    }
}

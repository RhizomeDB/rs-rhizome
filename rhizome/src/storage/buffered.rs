use std::collections::HashMap;

use anyhow::Result;
use cid::Cid;

use super::blockstore::Blockstore;

pub trait Buffered: Blockstore {
    fn flush(&self, root: &Cid) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct BufferedBlockstore<BS> {
    inner: BS,
    write: HashMap<Cid, Vec<u8>>,
}

impl<BS> BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    pub fn new(inner: BS) -> Self {
        Self {
            inner,
            write: Default::default(),
        }
    }

    pub fn into_inner(self) -> BS {
        self.inner
    }
}

impl<BS> Buffered for BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    fn flush(&self, _root: &Cid) -> Result<()> {
        todo!();
    }
}

impl<BS> Blockstore for BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    fn has(&self, _k: &Cid) -> Result<bool> {
        todo!();
    }

    fn get(&self, _k: &Cid) -> Result<Option<&[u8]>> {
        todo!()
    }

    fn put_keyed(&mut self, _k: &Cid, _block: &[u8]) -> Result<()> {
        todo!()
    }
}

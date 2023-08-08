use anyhow::Result;
use cid::Cid;
use std::{cell::RefCell, collections::HashMap};

use super::blockstore::Blockstore;

#[derive(Clone, Debug, Default)]
pub struct MemoryBlockstore {
    blocks: RefCell<HashMap<Cid, Vec<u8>>>,
}

impl MemoryBlockstore {
    pub fn new() -> Self {
        Self {
            blocks: RefCell::default(),
        }
    }
}

impl Blockstore for MemoryBlockstore {
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.blocks.borrow().contains_key(k))
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.borrow().get(k).cloned())
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> anyhow::Result<()> {
        self.blocks.borrow_mut().insert(*k, block.into());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::storage::{block::Block, codec::DagCbor};

    use super::*;

    #[test]
    fn test_bs() -> Result<()> {
        let bs = MemoryBlockstore::default();

        let cid1 = bs.put(
            cid::multihash::Code::Sha2_256,
            &Block::new(DagCbor, "Hello"),
        )?;

        let cid2 = bs.put(
            cid::multihash::Code::Sha2_256,
            &Block::new(DagCbor, b"World"),
        )?;

        let block1 = bs.get(&cid1)?.unwrap();
        let block2 = bs.get(&cid2)?.unwrap();

        assert_eq!(block1, b"Hello");
        assert_eq!(block2, b"World");

        Ok(())
    }
}

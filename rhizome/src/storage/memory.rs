use anyhow::Result;
use cid::Cid;
use std::collections::HashMap;

use super::blockstore::Blockstore;

#[derive(Clone, Debug, Default)]
pub struct MemoryBlockstore {
    blocks: HashMap<Cid, Vec<u8>>,
}

impl MemoryBlockstore {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::default(),
        }
    }
}

impl Blockstore for MemoryBlockstore {
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.blocks.contains_key(k))
    }

    fn get(&self, k: &Cid) -> Result<Option<&[u8]>> {
        Ok(self.blocks.get(k).map(|b| b.as_ref()))
    }

    fn put_keyed(&mut self, k: &Cid, block: &[u8]) -> anyhow::Result<()> {
        self.blocks.insert(*k, block.into());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{block::Block, codec::DagCbor};

    use super::*;

    #[test]
    fn test_bs() {
        let mut bs = MemoryBlockstore::default();

        let cid1 = bs
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, "Hello"),
            )
            .unwrap();

        let cid2 = bs
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, b"World"),
            )
            .unwrap();

        let block1 = bs.get(&cid1).unwrap().unwrap();
        let block2 = bs.get(&cid2).unwrap().unwrap();

        assert_eq!(block1, b"Hello");
        assert_eq!(block2, b"World");
    }
}

use cid::Cid;
use serde::{de::DeserializeOwned, Serialize};

use super::{block::Block, codec::Codec, DEFAULT_MULTIHASH};

pub trait ContentAddressable: Serialize + DeserializeOwned {
    fn cid<C>(&self, codec: C) -> Cid
    where
        C: Codec,
    {
        let bytes = C::to_vec(self).unwrap();
        let block = Block::new(codec, &bytes);

        block.cid(DEFAULT_MULTIHASH)
    }
}

impl<T> ContentAddressable for T where T: Serialize + DeserializeOwned {}

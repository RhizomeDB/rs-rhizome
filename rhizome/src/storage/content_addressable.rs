use cid::Cid;
use serde::{de::DeserializeOwned, Serialize};

use super::{block::Block, codec::Codec, DefaultCodec, DEFAULT_MULTIHASH};

pub trait ContentAddressable: Serialize + DeserializeOwned {
    fn cid(&self) -> Cid {
        let codec = DefaultCodec::default();
        let bytes = DefaultCodec::to_vec(self).unwrap();
        let block = Block::new(codec, &bytes);

        block.cid(DEFAULT_MULTIHASH)
    }
}

impl<T> ContentAddressable for T where T: Serialize + DeserializeOwned {}

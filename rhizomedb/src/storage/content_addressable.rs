use anyhow::Result;
use cid::Cid;
use serde::{de::DeserializeOwned, Serialize};

use super::{block::Block, codec::Codec, DefaultCodec, DEFAULT_MULTIHASH};

pub trait ContentAddressable: Serialize + DeserializeOwned {
    fn cid(&self) -> Result<Cid> {
        #[allow(unknown_lints, clippy::default_constructed_unit_structs)]
        let codec = DefaultCodec::default();
        let bytes = DefaultCodec::to_vec(self)?;
        let block = Block::new(codec, &bytes);

        Ok(block.cid(DEFAULT_MULTIHASH))
    }
}

impl<T> ContentAddressable for T where T: Serialize + DeserializeOwned {}

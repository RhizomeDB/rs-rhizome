use cid::multihash::{self, Code::Sha3_256};

use self::codec::DagCbor;

pub mod block;
pub mod blockstore;
// pub mod buffered;
pub mod codec;
pub mod content_addressable;
pub mod memory;

pub const DEFAULT_MULTIHASH: multihash::Code = Sha3_256;

pub type DefaultCodec = DagCbor;

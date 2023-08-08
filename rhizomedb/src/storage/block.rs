use cid::{
    multihash::{self, MultihashDigest},
    Cid,
};

use super::codec::Codec;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Block<C, D> {
    pub codec: C,
    pub data: D,
}

impl<C, D> Block<C, D>
where
    C: Codec,
    D: AsRef<[u8]>,
{
    pub fn new(codec: C, data: D) -> Self {
        Self { codec, data }
    }

    pub fn cid(&self, mh_code: multihash::Code) -> Cid {
        Cid::new_v1(C::CODE, mh_code.digest(self.data.as_ref()))
    }

    pub fn is_empty(&self) -> bool {
        self.data.as_ref().is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().len()
    }
}

impl<C, D> AsRef<[u8]> for Block<C, D>
where
    C: Codec,
    D: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

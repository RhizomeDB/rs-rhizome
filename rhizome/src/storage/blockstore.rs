use anyhow::Result;
use cid::{multihash, Cid};

use super::{block::Block, codec::Codec, content_addressable::ContentAddressable};

pub trait Blockstore: Default {
    fn has(&self, k: &Cid) -> Result<bool>;
    fn get(&self, k: &Cid) -> Result<Option<&[u8]>>;
    fn put_keyed(&mut self, k: &Cid, block: &[u8]) -> Result<()>;

    fn put<C, D>(&mut self, mh_code: multihash::Code, block: &Block<C, D>) -> Result<Cid>
    where
        C: Codec,
        D: AsRef<[u8]>,
    {
        let k = block.cid(mh_code);

        self.put_keyed(&k, block.as_ref())?;

        Ok(k)
    }

    fn put_many<C, D, I>(&mut self, blocks: I) -> Result<()>
    where
        C: Codec,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (multihash::Code, Block<C, D>)>,
    {
        self.put_many_keyed(blocks.into_iter().map(|(mc, b)| (b.cid(mc), b)))?;

        Ok(())
    }

    fn put_many_keyed<D, I>(&mut self, blocks: I) -> Result<()>
    where
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        for (c, b) in blocks {
            self.put_keyed(&c, b.as_ref())?
        }

        Ok(())
    }

    fn get_serializable<C, T>(&self, cid: &Cid) -> Result<Option<T>>
    where
        C: Codec,
        T: ContentAddressable,
    {
        match self.get(cid)? {
            Some(bz) => C::from_slice(bz).map(Some),
            None => Ok(None),
        }
    }

    fn put_serializable<C, T>(&mut self, obj: &T, codec: C, code: multihash::Code) -> Result<Cid>
    where
        C: Codec,
        T: ContentAddressable,
    {
        let bytes = C::to_vec(obj)?;

        self.put(code, &Block::new(codec, &bytes))
    }
}

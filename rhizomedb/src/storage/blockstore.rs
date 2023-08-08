use anyhow::Result;
use cid::{multihash, Cid};

use super::{block::Block, codec::Codec, content_addressable::ContentAddressable};

pub trait Blockstore {
    fn has(&self, k: &Cid) -> Result<bool>;
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>>;
    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()>;

    fn put<C, D>(&self, mh_code: multihash::Code, block: &Block<C, D>) -> Result<Cid>
    where
        C: Codec,
        D: AsRef<[u8]>,
    {
        let k = block.cid(mh_code);

        self.put_keyed(&k, block.as_ref())?;

        Ok(k)
    }

    fn put_many<C, D, I>(&self, blocks: I) -> Result<()>
    where
        C: Codec,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (multihash::Code, Block<C, D>)>,
    {
        self.put_many_keyed(blocks.into_iter().map(|(mc, b)| (b.cid(mc), b)))?;

        Ok(())
    }

    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
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
            Some(bz) => C::from_slice(bz.as_slice()).map(Some),
            None => Ok(None),
        }
    }

    fn put_serializable<C, T>(&self, obj: &T, codec: C, code: multihash::Code) -> Result<Cid>
    where
        C: Codec,
        T: ContentAddressable,
    {
        let bytes = C::to_vec(obj)?;

        self.put(code, &Block::new(codec, &bytes))
    }
}

impl<'a, T: Blockstore> Blockstore for &'a T {
    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }
}

impl<'a, T: Blockstore> Blockstore for &'a mut T {
    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }
}

impl<T: Blockstore> Blockstore for Box<T> {
    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }
}

impl<T: Blockstore> Blockstore for std::rc::Rc<T> {
    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }
}

impl<T: Blockstore> Blockstore for std::sync::Arc<T> {
    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }
}

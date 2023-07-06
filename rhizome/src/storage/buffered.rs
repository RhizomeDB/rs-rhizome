// This is pretty much copied from the reference implementation of FVM,
// and adapted to match the default multihash used by Rhizome, and the slight
// differences in the API for our blockstore.

use std::{
    cell::RefCell,
    collections::HashMap,
    io::{Cursor, Read, Seek},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use cid::Cid;

use crate::storage::codec::{Codec, DagCbor};

use super::blockstore::Blockstore;

pub trait Buffered: Blockstore {
    fn flush(&self, root: &Cid) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct BufferedBlockstore<BS> {
    inner: Arc<BS>,
    write: RefCell<HashMap<Cid, Vec<u8>>>,
}

impl<BS> BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    pub fn new(inner: Arc<BS>) -> Self {
        Self {
            inner,
            write: Default::default(),
        }
    }

    pub fn into_inner(self) -> Arc<BS> {
        self.inner
    }
}

impl<BS> Buffered for BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    fn flush(&self, root: &Cid) -> Result<()> {
        let mut buffer = Vec::new();
        let write = self.write.borrow_mut();

        copy_rec(&write, *root, &mut buffer)?;
        self.inner.put_many_keyed(buffer)?;

        Ok(())
    }
}

/// Given a CBOR encoded Buffer, returns a tuple of:
/// the type of the CBOR object along with extra
/// elements we expect to read. More info on this can be found in
/// Appendix C. of RFC 7049 which defines the CBOR specification.
/// This was implemented because the CBOR library we use does not expose low
/// methods like this, requiring us to deserialize the whole CBOR payload, which
/// is unnecessary and quite inefficient for our usecase here.
fn cbor_read_header_buf<B: Read>(br: &mut B, scratch: &mut [u8]) -> anyhow::Result<(u8, usize)> {
    let first = br.read_u8()?;
    let maj = (first & 0xe0) >> 5;
    let low = first & 0x1f;

    if low < 24 {
        Ok((maj, low as usize))
    } else if low == 24 {
        let val = br.read_u8()?;
        if val < 24 {
            return Err(anyhow!(
                "cbor input was not canonical (lval 24 with value < 24)"
            ));
        }
        Ok((maj, val as usize))
    } else if low == 25 {
        br.read_exact(&mut scratch[..2])?;
        let val = BigEndian::read_u16(&scratch[..2]);
        if val <= u8::MAX as u16 {
            return Err(anyhow!(
                "cbor input was not canonical (lval 25 with value <= MaxUint8)"
            ));
        }
        Ok((maj, val as usize))
    } else if low == 26 {
        br.read_exact(&mut scratch[..4])?;
        let val = BigEndian::read_u32(&scratch[..4]);
        if val <= u16::MAX as u32 {
            return Err(anyhow!(
                "cbor input was not canonical (lval 26 with value <= MaxUint16)"
            ));
        }
        Ok((maj, val as usize))
    } else if low == 27 {
        br.read_exact(&mut scratch[..8])?;
        let val = BigEndian::read_u64(&scratch[..8]);
        if val <= u32::MAX as u64 {
            return Err(anyhow!(
                "cbor input was not canonical (lval 27 with value <= MaxUint32)"
            ));
        }
        Ok((maj, val as usize))
    } else {
        Err(anyhow!("invalid header cbor_read_header_buf"))
    }
}

/// Given a CBOR serialized IPLD buffer, read through all of it and return all the Links.
/// This function is useful because it is quite a bit more fast than doing this recursively on a
/// deserialized IPLD object.
fn scan_for_links<B: Read + Seek, F>(buf: &mut B, mut callback: F) -> Result<()>
where
    F: FnMut(Cid) -> anyhow::Result<()>,
{
    let mut scratch: [u8; 100] = [0; 100];
    let mut remaining = 1;
    while remaining > 0 {
        let (maj, extra) = cbor_read_header_buf(buf, &mut scratch)?;
        match maj {
            // MajUnsignedInt, MajNegativeInt, MajOther
            0 | 1 | 7 => {}
            // MajByteString, MajTextString
            2 | 3 => {
                buf.seek(std::io::SeekFrom::Current(extra as i64))?;
            }
            // MajTag
            6 => {
                // Check if the tag refers to a CID
                if extra == 42 {
                    let (maj, extra) = cbor_read_header_buf(buf, &mut scratch)?;
                    // The actual CID is expected to be a byte string
                    if maj != 2 {
                        return Err(anyhow!("expected cbor type byte string in input"));
                    }
                    if extra > 100 {
                        return Err(anyhow!("string in cbor input too long"));
                    }
                    buf.read_exact(&mut scratch[..extra])?;
                    let c = Cid::try_from(&scratch[1..extra])?;
                    callback(c)?;
                } else {
                    remaining += 1;
                }
            }
            // MajArray
            4 => {
                remaining += extra;
            }
            // MajMap
            5 => {
                remaining += extra * 2;
            }
            _ => {
                return Err(anyhow!("unhandled cbor type: {}", maj));
            }
        }
        remaining -= 1;
    }

    Ok(())
}

/// Copies the IPLD DAG under `root` from the cache to the base store.
fn copy_rec<'a>(
    cache: &'a HashMap<Cid, Vec<u8>>,
    root: Cid,
    buffer: &mut Vec<(Cid, &'a [u8])>,
) -> Result<()> {
    const SHA2_256_CODE: u64 = 0x12;

    match (root.codec(), root.hash().code()) {
        // Allow Sha2_256 dag-cbor
        (DagCbor::CODE, SHA2_256_CODE) => (),
        (codec, hash) => {
            return Err(anyhow!(
                "cid {root} has unexpected codec ({codec}), hash ({hash}))"
            ))
        }
    }

    // If we don't have the block, we assume it's already in the datastore.
    let block = match cache.get(&root) {
        Some(blk) => blk,
        None => return Ok(()),
    };

    scan_for_links(&mut Cursor::new(block), |link| {
        copy_rec(cache, link, buffer)
    })?;

    buffer.push((root, block));

    Ok(())
}

impl<BS> Blockstore for BufferedBlockstore<BS>
where
    BS: Blockstore,
{
    fn has(&self, k: &Cid) -> Result<bool> {
        if self.write.borrow().contains_key(k) {
            Ok(true)
        } else {
            Ok(self.inner.has(k)?)
        }
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        if let Some(data) = self.write.borrow().get(k) {
            Ok(Some(data.clone()))
        } else {
            Ok(self.inner.get(k)?)
        }
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        self.write.borrow_mut().insert(*k, block.to_vec());

        Ok(())
    }

    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
    where
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        self.write
            .borrow_mut()
            .extend(blocks.into_iter().map(|(k, v)| (k, v.as_ref().into())));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use crate::storage::{block::Block, memory::MemoryBlockstore};

    use super::*;

    #[test]
    fn test_buffered_store() {
        let mem = Arc::new(MemoryBlockstore::default());
        let buf_store = BufferedBlockstore::new(Arc::clone(&mem));

        let cid = buf_store
            .put(cid::multihash::Code::Sha2_256, &Block::new(DagCbor, &[8]))
            .unwrap();

        assert_eq!(mem.get_serializable::<DagCbor, u8>(&cid).unwrap(), None);
        assert_eq!(
            buf_store.get_serializable::<DagCbor, u8>(&cid).unwrap(),
            Some(8)
        );

        buf_store.flush(&cid).unwrap();

        assert_eq!(mem.get_serializable::<DagCbor, u8>(&cid).unwrap(), Some(8));
        assert_eq!(
            buf_store.get_serializable::<DagCbor, u8>(&cid).unwrap(),
            Some(8)
        );
    }

    #[test]
    fn test_buffered_store_with_links() {
        let mem = Arc::new(MemoryBlockstore::default());
        let buf_store = BufferedBlockstore::new(Arc::clone(&mem));

        let str_val = String::from("value");
        let value = 8u8;
        let arr_cid = buf_store
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, DagCbor::to_vec(&(str_val.clone(), value)).unwrap()),
            )
            .unwrap();

        #[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
        struct TestObject {
            array: Cid,
            value: String,
        }

        let obj = TestObject {
            array: arr_cid,
            value: str_val.clone(),
        };

        let obj_cid = buf_store
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, DagCbor::to_vec(&obj).unwrap()),
            )
            .unwrap();

        let root_cid = buf_store
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, DagCbor::to_vec(&(obj_cid, 1u8)).unwrap()),
            )
            .unwrap();

        // Make sure a block not connected to the root does not get written
        let unconnected = buf_store
            .put(
                cid::multihash::Code::Sha2_256,
                &Block::new(DagCbor, DagCbor::to_vec(&27u8).unwrap()),
            )
            .unwrap();

        assert_eq!(
            mem.get_serializable::<DagCbor, TestObject>(&obj_cid)
                .unwrap(),
            None
        );
        assert_eq!(
            mem.get_serializable::<DagCbor, (Cid, u8)>(&root_cid)
                .unwrap(),
            None
        );
        assert_eq!(
            mem.get_serializable::<DagCbor, (String, u8)>(&arr_cid)
                .unwrap(),
            None
        );
        assert_eq!(
            buf_store
                .get_serializable::<DagCbor, u8>(&unconnected)
                .unwrap(),
            Some(27u8)
        );

        // Flush and assert changes
        buf_store.flush(&root_cid).unwrap();
        assert_eq!(
            mem.get_serializable::<DagCbor, (String, u8)>(&arr_cid)
                .unwrap(),
            Some((str_val, value))
        );
        assert_eq!(
            mem.get_serializable::<DagCbor, TestObject>(&obj_cid)
                .unwrap(),
            Some(obj)
        );
        assert_eq!(
            mem.get_serializable::<DagCbor, (Cid, u8)>(&root_cid)
                .unwrap(),
            Some((obj_cid, 1)),
        );
        assert_eq!(
            mem.get_serializable::<DagCbor, u8>(&unconnected).unwrap(),
            None
        );
    }
}

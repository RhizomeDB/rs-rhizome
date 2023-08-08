use anyhow::Result;
use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::{
    error::{error, Error},
    storage::{blockstore::Blockstore, codec::DagCbor, content_addressable::ContentAddressable},
    tuple::InputTuple,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Epoch {
    prev: Option<Cid>,
    tuples: Vec<Cid>,
}

impl Epoch {
    pub fn rewind<BS>(&self, bs: &BS) -> Result<Option<Self>>
    where
        BS: Blockstore,
    {
        if let Some(prev) = &self.prev {
            Ok(bs.get_serializable::<DagCbor, Epoch>(prev)?)
        } else {
            Ok(None)
        }
    }

    pub fn step_epoch(&self) -> Result<Self> {
        Ok(Self {
            prev: Some(self.cid()?),
            tuples: Vec::new(),
        })
    }

    pub fn push_tuple(&mut self, tuple: InputTuple) -> Result<()> {
        self.tuples.push(tuple.cid()?);

        Ok(())
    }

    pub fn has_tuples_pending(&self) -> bool {
        !self.tuples.is_empty()
    }

    pub fn with_tuples<BS, F>(&self, bs: &BS, f: &mut F) -> Result<()>
    where
        BS: Blockstore,
        F: FnMut(InputTuple) -> Result<()>,
    {
        for cid in &self.tuples {
            if let Some(tuple) = bs.get_serializable::<DagCbor, InputTuple>(cid)? {
                f(tuple)?;
            } else {
                return error(Error::InternalRhizomeError(
                    "expected block to deserialize as InputTuple".to_owned(),
                ));
            };
        }

        Ok(())
    }

    pub fn with_tuples_rec<BS, F>(&self, bs: &BS, f: &mut F) -> Result<()>
    where
        BS: Blockstore,
        F: FnMut(InputTuple) -> Result<()>,
    {
        let mut cur = Some(self.clone());

        while let Some(node) = cur {
            node.with_tuples(bs, f)?;

            if let Some(cid) = &node.prev {
                cur = bs.get_serializable::<DagCbor, Epoch>(cid)?;
            } else {
                cur = None;
            }
        }

        Ok(())
    }
}

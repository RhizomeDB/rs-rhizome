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
}

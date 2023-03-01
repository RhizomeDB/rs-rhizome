use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait Codec: Default {
    const CODE: u64;

    fn to_vec<T>(value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized;

    fn from_slice<'a, T>(slice: &'a [u8]) -> Result<T>
    where
        T: Deserialize<'a>;
}

#[derive(Debug, Default)]
pub struct DagCbor;

impl Codec for DagCbor {
    const CODE: u64 = 0x71;

    fn to_vec<T>(value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized,
    {
        serde_ipld_dagcbor::to_vec(value).map_err(Into::into)
    }

    fn from_slice<'a, T>(slice: &'a [u8]) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        serde_ipld_dagcbor::from_slice(slice).map_err(Into::into)
    }
}

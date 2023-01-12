use anyhow::Result;
use core::fmt::Debug;
use std::io::Write;

use crate::{
    error::{error, Error},
    fact::Fact,
};

pub trait Sink: Debug {
    // TODO: Return metadata about push on success?
    fn push(&mut self, f: Fact) -> Result<()>;

    // TODO: likely want a flush method to buffer pushes per epoch
}

pub struct WriteSink<W>
where
    W: Write,
{
    w: W,
}

impl<W> WriteSink<W>
where
    W: Write,
{
    pub fn new(w: W) -> Self {
        Self { w }
    }
}

impl<W> Sink for WriteSink<W>
where
    W: Write,
{
    fn push(&mut self, f: Fact) -> Result<()> {
        writeln!(self.w, "{f}").or_else(|_| error(Error::SinkPushError))
    }
}

impl<W> Debug for WriteSink<W>
where
    W: Write,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSink").finish()
    }
}

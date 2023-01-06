use anyhow::Result;
use core::fmt::Debug;

use crate::fact::Fact;

pub trait Source: Debug {
    // TODO: Possibly return an IntoIter instead
    fn pull(&mut self) -> Result<Vec<Fact<()>>>;
}

pub struct GeneratorSource<F>
where
    F: Fn() -> Result<Vec<Fact<()>>>,
{
    f: F,
}

impl<F> GeneratorSource<F>
where
    F: Fn() -> Result<Vec<Fact<()>>>,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> Source for GeneratorSource<F>
where
    F: Fn() -> Result<Vec<Fact<()>>>,
{
    fn pull(&mut self) -> Result<Vec<Fact<()>>> {
        (self.f)()
    }
}

impl<F> Debug for GeneratorSource<F>
where
    F: Fn() -> Result<Vec<Fact<()>>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratorSource").finish()
    }
}

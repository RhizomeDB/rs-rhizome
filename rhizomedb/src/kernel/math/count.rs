use std::ops::AddAssign;

use num_traits::{One, Zero};
use rhizomedb_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Count]
    fn count<T: RhizomeType + AddAssign + One + Zero>() -> T;
}

#[derive(Debug)]
pub struct Count<T>(T);

impl<T> Default for Count<T>
where
    T: RhizomeType + AddAssign + One + Zero,
{
    fn default() -> Self {
        Self(Zero::zero())
    }
}

impl<T> Aggregate for Count<T>
where
    T: RhizomeType + AddAssign + One + Zero,
{
    type Input = ();
    type Output = T;

    fn step(&mut self, _: ()) {
        self.0 += T::one();
    }

    fn finalize(&self) -> Option<Self::Output> {
        Some(self.0.clone())
    }
}

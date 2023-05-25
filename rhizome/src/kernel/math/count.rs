use std::ops::AddAssign;

use num_traits::{One, Zero};
use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Count]
    fn count<T: RhizomeType + AddAssign + One + Zero>() -> T;
}

#[derive(Debug)]
pub struct Count<T: RhizomeType + AddAssign + One + Zero>(T);

impl<T: RhizomeType + AddAssign + One + Zero> Default for Count<T> {
    fn default() -> Self {
        Self(Zero::zero())
    }
}

impl<T: RhizomeType + AddAssign + One + Zero> Aggregate for Count<T> {
    type Input = ();
    type Output = T;

    fn step(&mut self, _: ()) {
        self.0 += T::one();
    }

    fn finalize(&self) -> Option<Self::Output> {
        Some(self.0.clone())
    }
}

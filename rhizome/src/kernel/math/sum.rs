use std::ops::AddAssign;

use num_traits::Zero;
use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Sum]
    fn sum<T: RhizomeType + AddAssign + Zero>(arg: T) -> T;
}

#[derive(Debug)]
pub struct Sum<T: RhizomeType + AddAssign + Zero>(T);

impl<T: RhizomeType + AddAssign + Zero> Default for Sum<T> {
    fn default() -> Self {
        Self(Zero::zero())
    }
}

impl<T: RhizomeType + AddAssign + Zero> Aggregate for Sum<T> {
    type Input = (T,);
    type Output = T;

    fn step(&mut self, (t,): (T,)) {
        self.0 += t;
    }

    fn finalize(&self) -> Option<Self::Output> {
        Some(self.0.clone())
    }
}

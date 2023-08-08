use std::ops::AddAssign;

use num_traits::Zero;
use rhizomedb_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Sum]
    fn sum<T: RhizomeType + AddAssign + Zero>(arg: T) -> T;
}

#[derive(Debug)]
pub struct Sum<T>(T);

impl<T> Default for Sum<T>
where
    T: RhizomeType + AddAssign + Zero,
{
    fn default() -> Self {
        Self(Zero::zero())
    }
}

impl<T> Aggregate for Sum<T>
where
    T: RhizomeType + AddAssign + Zero,
{
    type Input = (T,);
    type Output = T;

    fn step(&mut self, (t,): (T,)) {
        self.0 += t;
    }

    fn finalize(&self) -> Option<Self::Output> {
        Some(self.0.clone())
    }
}

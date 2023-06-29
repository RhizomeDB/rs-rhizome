use std::ops::{AddAssign, Div};

use num_traits::{One, Zero};
use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Mean]
    fn mean<
        T: RhizomeType + AddAssign + Zero + One + Div<Output = T>
    >(arg: T) -> <T as Div>::Output;
}

#[derive(Debug)]
pub struct Mean<T>(T, T);

impl<T> Default for Mean<T>
where
    T: RhizomeType + AddAssign + Zero + One + Div<Output = T>,
{
    fn default() -> Self {
        Self(Zero::zero(), Zero::zero())
    }
}

impl<T> Aggregate for Mean<T>
where
    T: RhizomeType + AddAssign + Zero + One + Div<Output = T>,
{
    type Input = (T,);
    type Output = <T as Div>::Output;

    fn step(&mut self, (t,): (T,)) {
        self.0 += t;
        self.1 += One::one();
    }

    fn finalize(&self) -> Option<Self::Output> {
        Some(self.0.clone() / self.1.clone())
    }
}

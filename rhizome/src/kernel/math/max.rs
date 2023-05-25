use std::cmp;

use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Max]
    fn max<T: RhizomeType + Ord>(arg: T) -> T;
}

#[derive(Debug)]
pub struct Max<T: RhizomeType + Ord>(Option<T>);

impl<T: RhizomeType + Ord> Default for Max<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<T: RhizomeType + Ord> Aggregate for Max<T> {
    type Input = (T,);
    type Output = T;

    fn step(&mut self, (t,): (T,)) {
        let result = match self.0.take() {
            Some(v) => cmp::max(v, t),
            None => t,
        };

        self.0 = Some(result)
    }

    fn finalize(&self) -> Option<Self::Output> {
        self.0.clone()
    }
}

use std::cmp;

use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Min]
    fn min<T: RhizomeType + Ord>(arg: T) -> T;
}

#[derive(Debug)]
pub struct Min<T: RhizomeType + Ord>(Option<T>);

impl<T: RhizomeType + Ord> Default for Min<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<T: RhizomeType + Ord> Aggregate for Min<T> {
    type Input = (T,);
    type Output = T;

    fn step(&mut self, (t,): (T,)) {
        let result = match self.0.take() {
            Some(v) => cmp::min(v, t),
            None => t,
        };

        self.0 = Some(result)
    }

    fn finalize(&self) -> Option<Self::Output> {
        self.0.clone()
    }
}

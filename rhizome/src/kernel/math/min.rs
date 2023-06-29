use std::cmp;

use rhizome_macro::rhizome_fn;

use crate::{aggregation::Aggregate, types::RhizomeType};

rhizome_fn! {
    #[aggregate = Min]
    fn min<T: RhizomeType + Ord>(arg: T) -> T;
}

#[derive(Debug)]
pub struct Min<T>(Option<T>);

impl<T> Default for Min<T>
where
    T: RhizomeType + Ord,
{
    fn default() -> Self {
        Self(None)
    }
}

impl<T> Aggregate for Min<T>
where
    T: RhizomeType + Ord,
{
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

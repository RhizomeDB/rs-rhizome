use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use serde::Serialize;

use crate::lattice::Lattice;

pub trait Timestamp:
    Lattice + Serialize + Ord + Debug + Clone + Eq + Hash + Default + 'static
{
    type Epoch;
    type Iteration;

    fn epoch(&self) -> Self::Epoch;
    fn iteration(&self) -> Self::Iteration;

    fn clock_start(&self) -> Self;
    fn epoch_start(&self) -> Self;

    fn clock_end(&self) -> Self;
    fn epoch_end(&self) -> Self;

    fn advance_epoch(&self) -> Self;
    fn advance_iteration(&self) -> Self;
}

#[derive(Default, Serialize, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Hash)]
pub struct PairTimestamp(pub u32, pub u32);

impl Lattice for PairTimestamp {
    const TOP: Self = Self(u32::TOP, u32::TOP);
    const BOTTOM: Self = Self(u32::BOTTOM, u32::BOTTOM);

    fn cmp(&self, other: &Self) -> Option<Ordering> {
        Lattice::cmp(&(self.0, self.1), &(other.0, other.1))
    }

    fn join(&self, other: &Self) -> Self {
        let (epoch, iteration) = Lattice::join(&(self.0, self.1), &(other.0, other.1));

        Self(epoch, iteration)
    }

    fn meet(&self, other: &Self) -> Self {
        let (epoch, iteration) = Lattice::meet(&(self.0, self.1), &(other.0, other.1));

        Self(epoch, iteration)
    }
}

impl Timestamp for PairTimestamp {
    type Epoch = u32;
    type Iteration = u32;

    fn epoch(&self) -> Self::Epoch {
        self.0
    }

    fn iteration(&self) -> Self::Iteration {
        self.1
    }

    fn clock_start(&self) -> Self {
        Self(Self::Epoch::BOTTOM, Self::Iteration::BOTTOM)
    }

    fn epoch_start(&self) -> Self {
        Self(self.0, Self::Iteration::BOTTOM)
    }

    fn clock_end(&self) -> Self {
        Self(Self::Epoch::TOP, Self::Iteration::TOP)
    }

    fn epoch_end(&self) -> Self {
        Self(self.0, Self::Iteration::TOP)
    }

    fn advance_epoch(&self) -> Self {
        Self(self.0 + 1, Self::Iteration::BOTTOM)
    }

    fn advance_iteration(&self) -> Self {
        Self(self.0, self.1 + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_tests() {
        assert_eq!(0, PairTimestamp(0, 1).epoch());
        assert_eq!(1, PairTimestamp(0, 1).iteration());

        assert_eq!(PairTimestamp(0, 0), PairTimestamp(3, 2).clock_start());
        assert_eq!(PairTimestamp(3, 0), PairTimestamp(3, 2).epoch_start());

        assert_eq!(
            PairTimestamp(u32::MAX, u32::MAX),
            PairTimestamp(3, 2).clock_end()
        );
        assert_eq!(PairTimestamp(3, u32::MAX), PairTimestamp(3, 2).epoch_end());

        assert_eq!(PairTimestamp(4, 0), PairTimestamp(3, 2).advance_epoch());
        assert_eq!(PairTimestamp(3, 3), PairTimestamp(3, 2).advance_iteration());
    }
}

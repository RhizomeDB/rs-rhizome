use im::OrdSet;

use crate::{
    fact::Fact,
    timestamp::{DefaultTimestamp, Timestamp},
};

pub type DefaultRelation<T = DefaultTimestamp> = ImmutableOrdSetRelation<T>;

pub trait Relation<T = DefaultTimestamp>:
    IntoIterator<Item = Fact<T>> + FromIterator<Fact<T>> + Default + Clone + Eq + PartialEq
where
    T: Timestamp,
{
    fn new() -> Self {
        Default::default()
    }

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, fact: &Fact<T>) -> bool;
    fn insert(self, fact: Fact<T>) -> Self;
    fn merge(self, rhs: Self) -> Self;
}

// Just a simple (and slow) implementation for initial prototyping
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableOrdSetRelation<T: Timestamp> {
    inner: OrdSet<Fact<T>>,
}

impl<T: Timestamp> Relation<T> for ImmutableOrdSetRelation<T> {
    fn new() -> Self {
        Default::default()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn contains(&self, fact: &Fact<T>) -> bool {
        self.inner.contains(fact)
    }

    fn insert(self, fact: Fact<T>) -> Self {
        Self {
            inner: self.inner.update(fact),
        }
    }

    fn merge(self, rhs: Self) -> Self {
        Self {
            inner: self.inner.union(rhs.inner),
        }
    }
}

impl<T: Timestamp> IntoIterator for ImmutableOrdSetRelation<T> {
    type Item = Fact<T>;
    type IntoIter = im::ordset::ConsumingIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<TS: Timestamp> FromIterator<Fact<TS>> for ImmutableOrdSetRelation<TS> {
    fn from_iter<T: IntoIterator<Item = Fact<TS>>>(iter: T) -> Self {
        Self {
            inner: OrdSet::from_iter(iter),
        }
    }
}

impl<T: Timestamp> Default for ImmutableOrdSetRelation<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

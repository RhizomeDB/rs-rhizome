use im::HashSet;

use crate::{
    fact::Fact,
    timestamp::{DefaultTimestamp, Timestamp},
};

pub type DefaultRelation<T = DefaultTimestamp> = ImmutableHashSetRelation<T>;

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
pub struct ImmutableHashSetRelation<T: Timestamp> {
    hashset: HashSet<Fact<T>>,
}

impl<T: Timestamp> Relation<T> for ImmutableHashSetRelation<T> {
    fn new() -> Self {
        Default::default()
    }

    fn len(&self) -> usize {
        self.hashset.len()
    }

    fn is_empty(&self) -> bool {
        self.hashset.is_empty()
    }

    fn contains(&self, fact: &Fact<T>) -> bool {
        self.hashset.contains(fact)
    }

    fn insert(self, fact: Fact<T>) -> Self {
        Self {
            hashset: self.hashset.update(fact),
        }
    }

    fn merge(self, rhs: Self) -> Self {
        Self {
            hashset: self.hashset.union(rhs.hashset),
        }
    }
}

impl<T: Timestamp> IntoIterator for ImmutableHashSetRelation<T> {
    type Item = Fact<T>;
    type IntoIter = im::hashset::ConsumingIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.hashset.into_iter()
    }
}

impl<TS: Timestamp> FromIterator<Fact<TS>> for ImmutableHashSetRelation<TS> {
    fn from_iter<T: IntoIterator<Item = Fact<TS>>>(iter: T) -> Self {
        Self {
            hashset: HashSet::from_iter(iter),
        }
    }
}

impl<T: Timestamp> Default for ImmutableHashSetRelation<T> {
    fn default() -> Self {
        Self {
            hashset: Default::default(),
        }
    }
}

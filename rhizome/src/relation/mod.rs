use im::OrdSet;

use crate::fact::Fact;

pub type DefaultRelation = ImmutableOrdSetRelation;

// TODO: Keep track of the timestamp a fact was derived at
pub trait Relation:
    IntoIterator<Item = Fact> + FromIterator<Fact> + Default + Clone + Eq + PartialEq
{
    fn new() -> Self {
        Default::default()
    }

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, fact: &Fact) -> bool;
    fn insert(self, fact: Fact) -> Self;
    fn merge(self, rhs: Self) -> Self;
}

// Just a simple (and slow) implementation for initial prototyping
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImmutableOrdSetRelation {
    inner: OrdSet<Fact>,
}

impl Relation for ImmutableOrdSetRelation {
    fn new() -> Self {
        Default::default()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn contains(&self, fact: &Fact) -> bool {
        self.inner.contains(fact)
    }

    fn insert(self, fact: Fact) -> Self {
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

impl IntoIterator for ImmutableOrdSetRelation {
    type Item = Fact;
    type IntoIter = im::ordset::ConsumingIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl FromIterator<Fact> for ImmutableOrdSetRelation {
    fn from_iter<T: IntoIterator<Item = Fact>>(iter: T) -> Self {
        Self {
            inner: OrdSet::from_iter(iter),
        }
    }
}

use crate::fact::traits::Fact;
use im::OrdSet;
use std::hash::Hash;

pub type DefaultRelation<F> = ImmutableOrdSetRelation<F>;

pub trait RelationSource:
    Clone + Copy + Eq + PartialEq + Ord + PartialOrd + Hash + Default
{
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct EDB;
impl RelationSource for EDB {}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct IDB;
impl RelationSource for IDB {}

// TODO: Keep track of the timestamp a fact was derived at?
pub trait Relation<F>:
    Default + Clone + Eq + PartialEq + FromIterator<F> + IntoIterator<Item = F>
where
    F: Fact,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, fact: &F) -> bool;
    fn insert(self, fact: F) -> Self;
    fn merge(self, rhs: Self) -> Self;
}

// Just a simple (and slow) implementation for initial prototyping
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    inner: OrdSet<F>,
}

impl<F> Default for ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    fn default() -> Self {
        Self {
            inner: OrdSet::default(),
        }
    }
}

impl<F> Relation<F> for ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn contains(&self, fact: &F) -> bool {
        // TODO: This is incredibly slow. We either need to project relations down to the columns used in existence checks, or
        // only allow negation over the complete set of attributes for a relation.
        self.inner.iter().any(|f| {
            fact.attributes()
                .iter()
                .all(|(k, v)| f.attributes().get(k) == Some(v))
        })
    }

    fn insert(self, fact: F) -> Self {
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

impl<F> IntoIterator for ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    type Item = F;
    type IntoIter = im::ordset::ConsumingIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<F> FromIterator<F> for ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = F>,
    {
        Self {
            inner: OrdSet::from_iter(iter),
        }
    }
}

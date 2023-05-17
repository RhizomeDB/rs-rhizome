use crate::{fact::traits::Fact, id::ColId, value::Val};
use im::OrdSet;
use std::{fmt::Debug, hash::Hash};

pub(crate) type DefaultRelation<F> = ImmutableOrdSetRelation<F>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Source {
    Edb,
    Idb,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct EdbMarker;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct IdbMarker;

// TODO: Keep track of the timestamp a fact was derived at?
pub trait Relation: Default + Eq + PartialEq + Debug {
    type Fact: Fact;
    type Iter<'a>: Iterator<Item = &'a Self::Fact>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, bindings: &[(ColId, Val)]) -> bool;

    fn insert(&mut self, fact: Self::Fact);
    fn merge(&mut self, rhs: &Self);
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

impl<F> Relation for ImmutableOrdSetRelation<F>
where
    F: Fact,
{
    type Fact = F;
    type Iter<'a> = im::ordset::Iter<'a, F> where Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.inner.iter()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn contains(&self, bindings: &[(ColId, Val)]) -> bool {
        // TODO: This is incredibly slow. We either need to project relations down to the columns used in existence checks, or
        // only allow negation over the complete set of columns for a relation.
        self.inner.iter().any(|f| {
            bindings
                .iter()
                .all(|(k, v)| f.col(k).map_or(false, |b| *b == *v))
        })
    }

    fn insert(&mut self, fact: F) {
        self.inner = self.inner.update(fact);
    }

    fn merge(&mut self, rhs: &Self) {
        self.inner = self.inner.clone().union(rhs.inner.clone());
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

impl<'a, F> IntoIterator for &'a ImmutableOrdSetRelation<F>
where
    F: Fact + 'a,
{
    type Item = &'a F;
    type IntoIter = im::ordset::Iter<'a, F>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

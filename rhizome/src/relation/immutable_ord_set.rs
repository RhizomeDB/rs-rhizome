use im::OrdSet;

use crate::{fact::traits::Fact, id::ColId, value::Val};

use super::Relation;

// Just a simple (and slow) implementation for initial prototyping
#[derive(Clone, Debug)]
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

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        // TODO: This is incredibly slow. We either need to project relations down to the columns used in existence checks, or
        // only allow negation over the complete set of columns for a relation.
        self.inner.iter().any(|f| {
            bindings
                .iter()
                .all(|(k, v)| f.col(k).map_or(false, |b| *b == *v))
        })
    }

    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Self::Fact> + '_> {
        Box::new(self.inner.iter().filter(move |f| {
            bindings
                .iter()
                .all(|(k, v)| f.col(k).map_or(false, |b| *b == *v))
        }))
    }

    fn insert(&mut self, _bindings: Vec<(ColId, Val)>, val: F) {
        self.inner = self.inner.update(val);
    }

    fn merge(&self, rhs: &Self) -> Self {
        Self {
            inner: self.inner.clone().union(rhs.inner.clone()),
        }
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

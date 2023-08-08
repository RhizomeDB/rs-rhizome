use as_any::Downcast;
use im::OrdSet;

use crate::{id::ColId, tuple::Tuple, value::Val};

use super::Relation;

// Just a simple (and slow) implementation for initial prototyping
#[derive(Clone, Debug, Default)]
pub struct ImmutableOrdSetRelation {
    inner: OrdSet<Tuple>,
}

impl Relation for ImmutableOrdSetRelation {
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
                .all(|(k, v)| f.col(k).map_or(false, |b| b == *v))
        })
    }

    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Tuple> + '_> {
        Box::new(self.inner.iter().filter(move |f| {
            bindings
                .iter()
                .all(|(k, v)| f.col(k).map_or(false, |b| b == *v))
        }))
    }

    fn purge(&mut self) {
        self.inner = OrdSet::default();
    }

    fn insert(&mut self, _bindings: Vec<(ColId, Val)>, val: Tuple) {
        self.inner = self.inner.update(val);
    }

    fn merge(&mut self, rhs: &dyn Relation) {
        if let Some(rhs) = rhs.downcast_ref::<Self>() {
            self.inner.extend(rhs.inner.iter().cloned());
        } else {
            panic!("Attempted to merge incompatible relations");
        }
    }
}

impl FromIterator<Tuple> for ImmutableOrdSetRelation {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Tuple>,
    {
        Self {
            inner: OrdSet::from_iter(iter),
        }
    }
}

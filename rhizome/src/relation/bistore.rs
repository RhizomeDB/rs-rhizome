use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::Result;
use as_any::Downcast;
use derive_more::AsRef;

use crate::{
    error::{error, Error},
    id::ColId,
    tuple::Tuple,
    value::Val,
};

use super::Relation;

trait Key: Clone + Eq + PartialEq + Ord + PartialOrd + AsRef<Val> {}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, AsRef)]
struct FromKey(Val);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, AsRef)]
struct ToKey(Val);

impl Key for FromKey {}
impl Key for ToKey {}

type Leaf<V> = BTreeSet<Arc<V>>;
type Layer<K, V> = BTreeMap<K, V>;

type Index<K1, K2, V> = Layer<K1, Layer<K2, Leaf<V>>>;

#[derive(Debug, Clone)]
pub struct Bistore<T> {
    ft: Index<FromKey, ToKey, T>,
    tf: Index<ToKey, FromKey, T>,
}

impl<T> Default for Bistore<T> {
    fn default() -> Self {
        Self {
            ft: Default::default(),
            tf: Default::default(),
        }
    }
}

impl<T> Bistore<T>
where
    T: Ord + 'static,
{
    pub(crate) fn len(&self) -> usize {
        self.ft.values().fold(0, |acc, v1| {
            acc + v1.values().fold(0, |acc, v2| acc + v2.len())
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ft.is_empty()
    }

    pub(crate) fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        let (f, t) = Self::bindings_to_cols(bindings);

        match (f, t) {
            (None, None) => !self.is_empty(),
            (None, Some(t)) => Self::index_contains_1(&self.tf, t),
            (Some(f), None) => Self::index_contains_1(&self.ft, f),
            (Some(f), Some(t)) => Self::index_contains_2(&self.ft, f, t),
        }
    }

    pub(crate) fn purge(&mut self) {
        self.ft.clear();
        self.tf.clear();
    }

    pub(crate) fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: T) -> Result<()> {
        let val = Arc::new(val);
        let (f, t) = Self::bindings_to_cols(bindings);

        match (f, t) {
            (None, _) => {
                return error(Error::InternalRhizomeError(
                    "expected column: 'from'".to_owned(),
                ))
            }
            (_, None) => {
                return error(Error::InternalRhizomeError(
                    "expected column: 'to'".to_owned(),
                ))
            }
            (Some(f), Some(t)) => {
                // TODO: We can share the suffixes between the indices
                Self::index_insert(&mut self.ft, (f.clone(), t.clone()), Arc::clone(&val));
                Self::index_insert(&mut self.tf, (t, f), Arc::clone(&val));
            }
        }

        Ok(())
    }

    pub(crate) fn search(&self, bindings: Vec<(ColId, Val)>) -> BTreeSet<&T> {
        let (f, t) = Self::bindings_to_cols(bindings);

        match (f, t) {
            (None, None) => Self::index_search_0(&self.ft),
            (None, Some(t)) => Self::index_search_1(&self.tf, t),
            (Some(f), None) => Self::index_search_1(&self.ft, f),
            (Some(f), Some(t)) => Self::index_search_2(&self.ft, f, t),
        }
    }

    pub(crate) fn merge(&mut self, rhs: &Self) {
        Self::index_merge(&mut self.ft, &rhs.ft);
        Self::index_merge(&mut self.tf, &rhs.tf);
    }

    fn bindings_to_cols(bindings: Vec<(ColId, Val)>) -> (Option<FromKey>, Option<ToKey>) {
        let mut f = None;
        let mut t = None;

        for (col, val) in bindings {
            match col {
                c if c == ColId::new("from") => f = Some(FromKey(val)),
                c if c == ColId::new("to") => t = Some(ToKey(val)),
                _ => continue,
            }
        }

        (f, t)
    }

    fn index_contains_1<K1, K2>(index: &Index<K1, K2, T>, k1: K1) -> bool
    where
        K1: Key,
        K2: Key,
    {
        index.get(&k1).map_or(false, |v1| !v1.is_empty())
    }

    fn index_contains_2<K1, K2>(index: &Index<K1, K2, T>, k1: K1, k2: K2) -> bool
    where
        K1: Key,
        K2: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .map_or(false, |v2| !v2.is_empty())
    }

    fn index_insert<K1, K2>(index: &mut Index<K1, K2, T>, keys: (K1, K2), val: Arc<T>)
    where
        K1: Key,
        K2: Key,
    {
        let (k1, k2) = keys;

        match index.get_mut(&k1) {
            None => {
                index.insert(k1.clone(), Layer::from_iter([(k2, Leaf::from_iter([val]))]));
            }
            Some(v1) => match v1.get_mut(&k2) {
                None => {
                    v1.insert(k2, Leaf::from_iter([val]));
                }
                Some(v2) => {
                    v2.insert(val);
                }
            },
        };
    }

    fn index_search_0<K1, K2>(index: &Index<K1, K2, T>) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
    {
        index
            .values()
            .flat_map(|v1| v1.values().flat_map(|v2| v2.iter().map(Arc::as_ref)))
            .collect()
    }

    fn index_search_1<K1, K2>(index: &Index<K1, K2, T>, k1: K1) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
    {
        index.get(&k1).map_or(BTreeSet::new(), |v1| {
            v1.values()
                .flat_map(|v2| v2.iter().map(Arc::as_ref))
                .collect()
        })
    }

    fn index_search_2<K1, K2>(index: &Index<K1, K2, T>, k1: K1, k2: K2) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .map_or(BTreeSet::new(), |v2| v2.iter().map(Arc::as_ref).collect())
    }

    fn index_merge<K1, K2>(lhs: &mut Index<K1, K2, T>, rhs: &Index<K1, K2, T>)
    where
        K1: Key,
        K2: Key,
    {
        Self::index_merge_layer(lhs, rhs, |l1, r1| {
            Self::index_merge_layer(l1, r1, |l2, r2| l2.extend(r2.iter().cloned()))
        })
    }

    fn index_merge_layer<K, V, F>(lhs_layer: &mut Layer<K, V>, rhs_layer: &Layer<K, V>, f: F)
    where
        K: Key,
        V: Clone,
        F: Fn(&mut V, &V),
    {
        let mut new = Vec::default();

        {
            let mut lhs = lhs_layer.iter_mut();
            let mut rhs = rhs_layer.iter();
            let mut lhs_cur = lhs.next();
            let mut rhs_cur = rhs.next();

            loop {
                match (lhs_cur, rhs_cur) {
                    (None, None) => break,
                    (Some(_), None) => {
                        lhs_cur = lhs.next();
                        rhs_cur = None;
                    }
                    (None, Some((k2, v2))) => {
                        new.push((k2.clone(), v2.clone()));

                        lhs_cur = None;
                        rhs_cur = rhs.next();
                    }
                    (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                        Ordering::Less => {
                            lhs_cur = lhs.next();
                            rhs_cur = Some((k2, v2));
                        }
                        Ordering::Greater => {
                            new.push((k2.clone(), v2.clone()));

                            lhs_cur = Some((k1, v1));
                            rhs_cur = rhs.next();
                        }
                        Ordering::Equal => {
                            f(v1, v2);

                            lhs_cur = lhs.next();
                            rhs_cur = rhs.next();
                        }
                    },
                }
            }
        }

        lhs_layer.extend(new);
    }
}

impl Relation for Bistore<Tuple> {
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        self.contains(bindings)
    }

    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Tuple> + '_> {
        let iterator = self.search(bindings).into_iter();

        Box::new(iterator)
    }

    fn purge(&mut self) {
        self.purge();
    }

    fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: Tuple) {
        self.insert(bindings, val).unwrap()
    }

    fn merge(&mut self, rhs: &dyn Relation) {
        if let Some(rhs) = rhs.downcast_ref::<Self>() {
            self.merge(rhs)
        } else {
            panic!("Attempted to merge incompatible relations");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use crate::id::ColId;

    use super::Bistore;

    #[test]
    fn test_len() -> Result<()> {
        let mut bistore = Bistore::<usize>::default();

        assert_eq!(bistore.len(), 0);

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        assert_eq!(bistore.len(), 1);

        // Same bindings, different order
        bistore.insert(
            vec![(ColId::new("to"), 1.into()), (ColId::new("from"), 0.into())],
            0,
        )?;

        assert_eq!(bistore.len(), 1);

        // Same bindings, different value
        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            1,
        )?;

        assert_eq!(bistore.len(), 2);

        // Different from binding
        bistore.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        assert_eq!(bistore.len(), 3);

        // Different to binding
        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 3.into())],
            0,
        )?;

        assert_eq!(bistore.len(), 4);

        Ok(())
    }

    #[test]
    fn test_is_empty() -> Result<()> {
        let mut bistore = Bistore::<usize>::default();

        assert_eq!(bistore.is_empty(), true);

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        assert_eq!(bistore.is_empty(), false);

        Ok(())
    }

    #[test]
    fn test_contains() -> Result<()> {
        let mut bistore = Bistore::<usize>::default();

        assert_eq!(bistore.contains(vec![]), false);

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        // Contains the complete set of the bindings
        assert_eq!(
            bistore.contains(vec![
                (ColId::new("from"), 0.into()),
                (ColId::new("to"), 1.into()),
            ],),
            true
        );

        // Contains each subset of the bindings
        assert_eq!(
            bistore.contains(vec![(ColId::new("from"), 0.into()),],),
            true
        );

        assert_eq!(bistore.contains(vec![(ColId::new("to"), 1.into()),],), true);

        // Does not contain any other combinations of bindings
        assert_eq!(
            bistore.contains(vec![(ColId::new("from"), 1.into()),],),
            false
        );

        assert_eq!(
            bistore.contains(vec![(ColId::new("to"), 3.into()),],),
            false
        );

        assert_eq!(
            bistore.contains(vec![
                (ColId::new("from"), 0.into()),
                (ColId::new("to"), 3.into()),
            ],),
            false
        );

        assert_eq!(
            bistore.contains(vec![
                (ColId::new("from"), 2.into()),
                (ColId::new("to"), 1.into()),
            ],),
            false
        );

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 2.into())],
            0,
        )?;

        bistore.insert(
            vec![(ColId::new("from"), 1.into()), (ColId::new("to"), 2.into())],
            0,
        )?;

        // Now contains the newly matching bindings
        assert_eq!(
            bistore.contains(vec![
                (ColId::new("from"), 0.into()),
                (ColId::new("to"), 2.into()),
            ],),
            true
        );

        assert_eq!(
            bistore.contains(vec![
                (ColId::new("from"), 1.into()),
                (ColId::new("to"), 2.into()),
            ],),
            true
        );

        Ok(())
    }

    #[test]
    fn test_search() -> Result<()> {
        let mut bistore = Bistore::<usize>::default();

        assert_eq!(bistore.search(vec![]), BTreeSet::new());

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        bistore.insert(
            vec![(ColId::new("from"), 1.into()), (ColId::new("to"), 2.into())],
            1,
        )?;

        bistore.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 3.into())],
            2,
        )?;

        bistore.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 2.into())],
            3,
        )?;

        bistore.insert(
            vec![(ColId::new("from"), 1.into()), (ColId::new("to"), 3.into())],
            4,
        )?;

        assert_eq!(
            bistore.search(vec![]),
            BTreeSet::from_iter(&[0, 1, 2, 3, 4])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("from"), 0.into())]),
            BTreeSet::from_iter(&[0, 3])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("from"), 1.into())]),
            BTreeSet::from_iter(&[1, 4])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("from"), 2.into())]),
            BTreeSet::from_iter(&[2])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("to"), 1.into())]),
            BTreeSet::from_iter(&[0])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("to"), 2.into())]),
            BTreeSet::from_iter(&[1, 3])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("to"), 3.into())]),
            BTreeSet::from_iter(&[2, 4])
        );

        assert_eq!(
            bistore.search(vec![(ColId::new("to"), 5.into())]),
            BTreeSet::from_iter(&[])
        );

        Ok(())
    }

    #[test]
    fn test_merge_into() -> Result<()> {
        let mut bistore1 = Bistore::<usize>::default();
        let mut bistore2 = Bistore::<usize>::default();

        bistore1.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 1.into())],
            0,
        )?;

        bistore1.insert(
            vec![(ColId::new("from"), 1.into()), (ColId::new("to"), 2.into())],
            1,
        )?;

        bistore1.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 3.into())],
            2,
        )?;

        bistore1.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 4.into())],
            6,
        )?;

        bistore2.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 3.into())],
            2,
        )?;

        bistore2.insert(
            vec![(ColId::new("from"), 0.into()), (ColId::new("to"), 2.into())],
            3,
        )?;

        bistore2.insert(
            vec![(ColId::new("from"), 1.into()), (ColId::new("to"), 3.into())],
            4,
        )?;

        bistore2.insert(
            vec![(ColId::new("from"), 2.into()), (ColId::new("to"), 4.into())],
            5,
        )?;

        bistore1.merge(&bistore2);

        assert_eq!(bistore1.len(), 7);

        assert_eq!(
            bistore1.search(vec![]),
            BTreeSet::from_iter(&[0, 1, 2, 3, 4, 5, 6])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("from"), 0.into())]),
            BTreeSet::from_iter(&[0, 3])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("from"), 1.into())]),
            BTreeSet::from_iter(&[1, 4])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("from"), 2.into())]),
            BTreeSet::from_iter(&[2, 5, 6])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("to"), 1.into())]),
            BTreeSet::from_iter(&[0])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("to"), 2.into())]),
            BTreeSet::from_iter(&[1, 3])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("to"), 3.into())]),
            BTreeSet::from_iter(&[2, 4])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("to"), 4.into())]),
            BTreeSet::from_iter(&[5, 6])
        );

        assert_eq!(
            bistore1.search(vec![(ColId::new("to"), 5.into())]),
            BTreeSet::from_iter(&[])
        );

        Ok(())
    }
}

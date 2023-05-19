use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::Result;
use derive_more::AsRef;

use crate::{
    error::{error, Error},
    fact::traits::EDBFact,
    id::ColId,
    value::Val,
};

use super::Relation;

trait Key: Clone + Eq + PartialEq + Ord + PartialOrd + AsRef<Val> {}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, AsRef)]
struct EntityKey(Val);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, AsRef)]
struct AttributeKey(Val);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, AsRef)]
struct ValueKey(Val);

impl Key for EntityKey {}
impl Key for AttributeKey {}
impl Key for ValueKey {}

type Leaf<V> = BTreeSet<Arc<V>>;
type Layer<K, V> = BTreeMap<K, V>;

type Index<K1, K2, K3, V> = Layer<K1, Layer<K2, Layer<K3, Leaf<V>>>>;

#[derive(Debug, Clone)]
pub struct Hexastore<T> {
    eav: Index<EntityKey, AttributeKey, ValueKey, T>,
    eva: Index<EntityKey, ValueKey, AttributeKey, T>,
    aev: Index<AttributeKey, EntityKey, ValueKey, T>,
    ave: Index<AttributeKey, ValueKey, EntityKey, T>,
    vea: Index<ValueKey, EntityKey, AttributeKey, T>,
    vae: Index<ValueKey, AttributeKey, EntityKey, T>,
}

impl<T> Default for Hexastore<T>
where
    T: Ord,
{
    fn default() -> Self {
        Self {
            eav: Default::default(),
            eva: Default::default(),
            aev: Default::default(),
            ave: Default::default(),
            vea: Default::default(),
            vae: Default::default(),
        }
    }
}

impl<T> Hexastore<T>
where
    T: Ord,
{
    pub(crate) fn len(&self) -> usize {
        self.eva.values().fold(0, |acc, v1| {
            acc + v1.values().fold(0, |acc, v2| {
                acc + v2.values().fold(0, |acc, v3| acc + v3.len())
            })
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.eva.is_empty()
    }

    pub(crate) fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        let (e, a, v) = Self::bindings_to_cols(bindings);

        match (e, a, v) {
            (None, None, None) => !self.is_empty(),
            (None, None, Some(v)) => Self::index_contains_1(&self.vae, v),
            (None, Some(a), None) => Self::index_contains_1(&self.aev, a),
            (None, Some(a), Some(v)) => Self::index_contains_2(&self.ave, a, v),
            (Some(e), None, None) => Self::index_contains_1(&self.eav, e),
            (Some(e), None, Some(v)) => Self::index_contains_2(&self.eva, e, v),
            (Some(e), Some(a), None) => Self::index_contains_2(&self.eav, e, a),
            (Some(e), Some(a), Some(v)) => Self::index_contains_3(&self.eav, e, a, v),
        }
    }

    pub(crate) fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: T) -> Result<()> {
        let val = Arc::new(val);
        let (e, a, v) = Self::bindings_to_cols(bindings);

        match (e, a, v) {
            (None, _, _) => {
                return error(Error::InternalRhizomeError(
                    "expected column: 'entity'".to_owned(),
                ))
            }
            (_, None, _) => {
                return error(Error::InternalRhizomeError(
                    "expected column: 'attribute'".to_owned(),
                ))
            }
            (_, _, None) => {
                return error(Error::InternalRhizomeError(
                    "expected column: 'value'".to_owned(),
                ))
            }
            (Some(e), Some(a), Some(v)) => {
                // TODO: We can share some suffixes between the indices
                // For example, eav and aev share the same list of values
                Self::index_insert(
                    &mut self.eav,
                    (e.clone(), a.clone(), v.clone()),
                    Arc::clone(&val),
                );
                Self::index_insert(
                    &mut self.eva,
                    (e.clone(), v.clone(), a.clone()),
                    Arc::clone(&val),
                );
                Self::index_insert(
                    &mut self.aev,
                    (a.clone(), e.clone(), v.clone()),
                    Arc::clone(&val),
                );
                Self::index_insert(
                    &mut self.ave,
                    (a.clone(), v.clone(), e.clone()),
                    Arc::clone(&val),
                );
                Self::index_insert(
                    &mut self.vea,
                    (v.clone(), e.clone(), a.clone()),
                    Arc::clone(&val),
                );
                Self::index_insert(&mut self.vae, (v, a, e), Arc::clone(&val));
            }
        }

        Ok(())
    }

    pub(crate) fn search(&self, bindings: Vec<(ColId, Val)>) -> BTreeSet<&T> {
        let (e, a, v) = Self::bindings_to_cols(bindings);

        match (e, a, v) {
            (None, None, None) => Self::index_search_0(&self.eav),
            (None, None, Some(v)) => Self::index_search_1(&self.vae, v),
            (None, Some(a), None) => Self::index_search_1(&self.aev, a),
            (None, Some(a), Some(v)) => Self::index_search_2(&self.ave, a, v),
            (Some(e), None, None) => Self::index_search_1(&self.eav, e),
            (Some(e), None, Some(v)) => Self::index_search_2(&self.eva, e, v),
            (Some(e), Some(a), None) => Self::index_search_2(&self.eav, e, a),
            (Some(e), Some(a), Some(v)) => Self::index_search_3(&self.eav, e, a, v),
        }
    }

    pub(crate) fn merge(&self, other: &Self) -> Self {
        Self {
            eav: Self::index_merge(&self.eav, &other.eav),
            eva: Self::index_merge(&self.eva, &other.eva),
            aev: Self::index_merge(&self.aev, &other.aev),
            ave: Self::index_merge(&self.ave, &other.ave),
            vea: Self::index_merge(&self.vea, &other.vea),
            vae: Self::index_merge(&self.vae, &other.vae),
        }
    }

    fn bindings_to_cols(
        bindings: Vec<(ColId, Val)>,
    ) -> (Option<EntityKey>, Option<AttributeKey>, Option<ValueKey>) {
        let mut e = None;
        let mut a = None;
        let mut v = None;

        for (col, val) in bindings {
            match col {
                c if c == ColId::new("entity") => e = Some(EntityKey(val)),
                c if c == ColId::new("attribute") => a = Some(AttributeKey(val)),
                c if c == ColId::new("value") => v = Some(ValueKey(val)),
                _ => continue,
            }
        }

        (e, a, v)
    }

    fn index_contains_1<K1, K2, K3>(index: &Index<K1, K2, K3, T>, k1: K1) -> bool
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index.get(&k1).map_or(false, |v1| !v1.is_empty())
    }

    fn index_contains_2<K1, K2, K3>(index: &Index<K1, K2, K3, T>, k1: K1, k2: K2) -> bool
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .map_or(false, |v2| !v2.is_empty())
    }

    fn index_contains_3<K1, K2, K3>(index: &Index<K1, K2, K3, T>, k1: K1, k2: K2, k3: K3) -> bool
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .and_then(|v2| v2.get(&k3))
            .map_or(false, |v3| !v3.is_empty())
    }

    fn index_insert<K1, K2, K3>(index: &mut Index<K1, K2, K3, T>, keys: (K1, K2, K3), val: Arc<T>)
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        let (k1, k2, k3) = keys;

        match index.get_mut(&k1) {
            None => {
                index.insert(
                    k1.clone(),
                    Layer::from_iter([(
                        k2.clone(),
                        Layer::from_iter([(k3, Leaf::from_iter([val]))]),
                    )]),
                );
            }
            Some(v1) => match v1.get_mut(&k2) {
                None => {
                    v1.insert(
                        k2.clone(),
                        Layer::from_iter([(k3.clone(), Leaf::from_iter([val]))]),
                    );
                }
                Some(v2) => match v2.get_mut(&k3) {
                    None => {
                        v2.insert(k3.clone(), Leaf::from_iter([val]));
                    }
                    Some(v3) => {
                        v3.insert(val);
                    }
                },
            },
        };
    }

    fn index_search_0<K1, K2, K3>(index: &Index<K1, K2, K3, T>) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index
            .values()
            .flat_map(|v1| {
                v1.values()
                    .flat_map(|v2| v2.values().flat_map(|v3| v3.iter().map(Arc::as_ref)))
            })
            .collect()
    }

    fn index_search_1<K1, K2, K3>(index: &Index<K1, K2, K3, T>, k1: K1) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index.get(&k1).map_or(BTreeSet::new(), |v1| {
            v1.values()
                .flat_map(|v2| v2.values().flat_map(|v3| v3.iter().map(Arc::as_ref)))
                .collect()
        })
    }

    fn index_search_2<K1, K2, K3>(index: &Index<K1, K2, K3, T>, k1: K1, k2: K2) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .map_or(BTreeSet::new(), |v2| {
                v2.values()
                    .flat_map(|v3| v3.iter().map(Arc::as_ref))
                    .collect()
            })
    }

    fn index_search_3<K1, K2, K3>(
        index: &Index<K1, K2, K3, T>,
        k1: K1,
        k2: K2,
        k3: K3,
    ) -> BTreeSet<&T>
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        index
            .get(&k1)
            .and_then(|v1| v1.get(&k2))
            .and_then(|v2| v2.get(&k3))
            .map_or(BTreeSet::new(), |v3| v3.iter().map(Arc::as_ref).collect())
    }

    fn index_merge<K1, K2, K3>(
        lhs: &Index<K1, K2, K3, T>,
        rhs: &Index<K1, K2, K3, T>,
    ) -> Index<K1, K2, K3, T>
    where
        K1: Key,
        K2: Key,
        K3: Key,
    {
        Self::index_merge_layer(lhs, rhs, |l1, r1| {
            Self::index_merge_layer(l1, r1, |l2, r2| {
                Self::index_merge_layer(l2, r2, |l3, r3| BTreeSet::union(l3, r3).cloned().collect())
            })
        })
    }

    fn index_merge_layer<K, V, F>(lhs: &Layer<K, V>, rhs: &Layer<K, V>, f: F) -> Layer<K, V>
    where
        K: Key,
        V: Clone,
        F: Fn(&V, &V) -> V,
    {
        let mut r = Layer::new();

        let mut lhs = lhs.iter();
        let mut rhs = rhs.iter();
        let mut lhs_cur = lhs.next();
        let mut rhs_cur = rhs.next();

        loop {
            match (lhs_cur, rhs_cur) {
                (None, None) => break,
                (Some((k1, m1)), None) => {
                    r.insert(k1.clone(), m1.clone());

                    lhs_cur = lhs.next();
                    rhs_cur = None;
                }
                (None, Some((k2, m2))) => {
                    r.insert(k2.clone(), m2.clone());

                    lhs_cur = None;
                    rhs_cur = rhs.next();
                }
                (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                    Ordering::Less => {
                        r.insert(k1.clone(), v1.clone());

                        lhs_cur = lhs.next();
                        rhs_cur = Some((k2, v2));
                    }
                    Ordering::Greater => {
                        r.insert(k2.clone(), v2.clone());

                        lhs_cur = Some((k1, v1));
                        rhs_cur = rhs.next();
                    }
                    Ordering::Equal => {
                        r.insert(k1.clone(), f(v1, v2));

                        lhs_cur = lhs.next();
                        rhs_cur = rhs.next();
                    }
                },
            }
        }

        r
    }
}

impl<T> Relation for Hexastore<T>
where
    T: EDBFact,
{
    type Fact = T;

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool {
        self.contains(bindings)
    }

    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Self::Fact> + '_> {
        let iterator = self.search(bindings).into_iter();

        Box::new(iterator)
    }

    fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: Self::Fact) {
        self.insert(bindings, val).unwrap()
    }

    fn merge(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use crate::id::ColId;

    use super::Hexastore;

    #[test]
    fn test_len() -> Result<()> {
        let mut hexastore = Hexastore::<usize>::default();

        assert_eq!(hexastore.len(), 0);

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.len(), 1);

        // Same bindings, different order
        hexastore.insert(
            vec![
                (ColId::new("value"), "quinn".into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("entity"), 0.into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.len(), 1);

        // Same bindings, different value
        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            1,
        )?;

        assert_eq!(hexastore.len(), 2);

        // Different entity binding
        hexastore.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.len(), 3);

        // Different attribute binding
        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "age".into()),
                (ColId::new("value"), "30".into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.len(), 4);

        // Different value binding
        hexastore.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.len(), 5);

        Ok(())
    }

    #[test]
    fn test_is_empty() -> Result<()> {
        let mut hexastore = Hexastore::<usize>::default();

        assert_eq!(hexastore.is_empty(), true);

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        assert_eq!(hexastore.is_empty(), false);

        Ok(())
    }

    #[test]
    fn test_contains() -> Result<()> {
        let mut hexastore = Hexastore::<usize>::default();

        assert_eq!(hexastore.contains(vec![]), false);

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        // Contains the complete set of the bindings
        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],),
            true
        );

        // Contains each subset of the bindings
        assert_eq!(
            hexastore.contains(vec![(ColId::new("entity"), 0.into()),],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![(ColId::new("attribute"), "name".into()),],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![(ColId::new("value"), "quinn".into()),],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
            ],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("value"), "quinn".into()),
            ],),
            true
        );

        // Does not contain any other combinations of bindings
        assert_eq!(
            hexastore.contains(vec![(ColId::new("entity"), 1.into()),],),
            false
        );

        assert_eq!(
            hexastore.contains(vec![(ColId::new("attribute"), "age".into()),],),
            false
        );

        assert_eq!(
            hexastore.contains(vec![(ColId::new("value"), "brooke".into()),],),
            false
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "age".into()),
            ],),
            false
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],),
            false
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("value"), "quinn".into()),
            ],),
            false
        );

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "age".into()),
                (ColId::new("value"), 30.into()),
            ],
            0,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],
            0,
        )?;

        // Now contains the newly matching bindings
        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "age".into()),
                (ColId::new("value"), 30.into()),
            ],),
            true
        );

        assert_eq!(
            hexastore.contains(vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],),
            true
        );

        Ok(())
    }

    #[test]
    fn test_search() -> Result<()> {
        let mut hexastore = Hexastore::<usize>::default();

        assert_eq!(hexastore.search(vec![]), BTreeSet::new());

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],
            1,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "boris".into()),
            ],
            2,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "USA".into()),
            ],
            3,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "canada".into()),
            ],
            4,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "canada".into()),
            ],
            5,
        )?;

        hexastore.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "canada".into()),
            ],
            6,
        )?;

        assert_eq!(
            hexastore.search(vec![]),
            BTreeSet::from_iter(&[0, 1, 2, 3, 4, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 0.into())]),
            BTreeSet::from_iter(&[0, 3])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 1.into())]),
            BTreeSet::from_iter(&[1, 4])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 2.into())]),
            BTreeSet::from_iter(&[2, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "name".into())]),
            BTreeSet::from_iter(&[0, 1, 2])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "residence".into())]),
            BTreeSet::from_iter(&[3, 4, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "age".into())]),
            BTreeSet::from_iter(&[])
        );

        Ok(())
    }

    #[test]
    fn test_merge_into() -> Result<()> {
        let mut hexastore1 = Hexastore::<usize>::default();
        let mut hexastore2 = Hexastore::<usize>::default();

        hexastore1.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "quinn".into()),
            ],
            0,
        )?;

        hexastore1.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "brooke".into()),
            ],
            1,
        )?;

        hexastore1.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "boris".into()),
            ],
            2,
        )?;

        hexastore1.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "germany".into()),
            ],
            6,
        )?;

        hexastore2.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "name".into()),
                (ColId::new("value"), "boris".into()),
            ],
            2,
        )?;

        hexastore2.insert(
            vec![
                (ColId::new("entity"), 0.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "USA".into()),
            ],
            3,
        )?;

        hexastore2.insert(
            vec![
                (ColId::new("entity"), 1.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "canada".into()),
            ],
            4,
        )?;

        hexastore2.insert(
            vec![
                (ColId::new("entity"), 2.into()),
                (ColId::new("attribute"), "residence".into()),
                (ColId::new("value"), "canada".into()),
            ],
            5,
        )?;

        let hexastore = hexastore1.merge(&hexastore2);

        assert_eq!(hexastore.len(), 7);

        assert_eq!(
            hexastore.search(vec![]),
            BTreeSet::from_iter(&[0, 1, 2, 3, 4, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 0.into())]),
            BTreeSet::from_iter(&[0, 3])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 1.into())]),
            BTreeSet::from_iter(&[1, 4])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("entity"), 2.into())]),
            BTreeSet::from_iter(&[2, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "name".into())]),
            BTreeSet::from_iter(&[0, 1, 2])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "residence".into())]),
            BTreeSet::from_iter(&[3, 4, 5, 6])
        );

        assert_eq!(
            hexastore.search(vec![(ColId::new("attribute"), "age".into())]),
            BTreeSet::from_iter(&[])
        );

        Ok(())
    }
}

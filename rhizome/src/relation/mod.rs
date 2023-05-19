use crate::{fact::traits::Fact, id::ColId, value::Val};
use std::{fmt::Debug, hash::Hash};

pub(crate) mod hexastore;
pub(crate) mod immutable_ord_set;

pub(crate) use hexastore::*;
pub(crate) use immutable_ord_set::*;

pub(crate) type DefaultEDBRelation<F> = Hexastore<F>;
pub(crate) type DefaultIDBRelation<F> = ImmutableOrdSetRelation<F>;

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
pub trait Relation: Default + Debug {
    type Fact: Fact;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn contains(&self, bindings: Vec<(ColId, Val)>) -> bool;
    fn search(&self, bindings: Vec<(ColId, Val)>) -> Box<dyn Iterator<Item = &'_ Self::Fact> + '_>;

    fn insert(&mut self, bindings: Vec<(ColId, Val)>, val: Self::Fact);
    fn merge(&self, rhs: &Self) -> Self;
}

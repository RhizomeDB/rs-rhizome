use std::{
    cmp::{max_by, min_by},
    fmt::{self, Display},
    sync::Arc,
};

use crate::{types::Type, value::Val};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AggregationFunction {
    Count(Count),
    Sum(Sum),
    Min(Min),
    Max(Max),
}

impl AggregationFunction {
    pub fn count() -> Self {
        Self::Count(Count {})
    }

    pub fn sum() -> Self {
        Self::Sum(Sum {})
    }

    pub fn min() -> Self {
        Self::Min(Min {})
    }

    pub fn max() -> Self {
        Self::Max(Max {})
    }

    pub fn start(&self, target_type: Type) -> Option<Arc<Val>> {
        match self {
            AggregationFunction::Count(inner) => inner.start(target_type),
            AggregationFunction::Sum(inner) => inner.start(target_type),
            AggregationFunction::Min(inner) => inner.start(target_type),
            AggregationFunction::Max(inner) => inner.start(target_type),
        }
    }

    pub fn apply(&self, cur: Option<Arc<Val>>, new: Arc<Val>) -> Option<Arc<Val>> {
        match self {
            AggregationFunction::Count(inner) => inner.apply(cur, new),
            AggregationFunction::Sum(inner) => inner.apply(cur, new),
            AggregationFunction::Min(inner) => inner.apply(cur, new),
            AggregationFunction::Max(inner) => inner.apply(cur, new),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Count {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Sum {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Min {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Max {}

pub trait AggregationFn {
    fn start(&self, target_type: Type) -> Option<Arc<Val>>;
    fn apply(&self, cur: Option<Arc<Val>>, new: Arc<Val>) -> Option<Arc<Val>>;
}

impl AggregationFn for Count {
    fn start(&self, target_type: Type) -> Option<Arc<Val>> {
        target_type.zero().map(Arc::new)
    }

    fn apply(&self, cur: Option<Arc<Val>>, _new: Arc<Val>) -> Option<Arc<Val>> {
        match &*cur.unwrap() {
            Val::Bool(_) => panic!(),
            Val::S8(v) => Some(Arc::new((v + 1).into())),
            Val::U8(v) => Some(Arc::new((v + 1).into())),
            Val::S16(v) => Some(Arc::new((v + 1).into())),
            Val::U16(v) => Some(Arc::new((v + 1).into())),
            Val::S32(v) => Some(Arc::new((v + 1).into())),
            Val::U32(v) => Some(Arc::new((v + 1).into())),
            Val::S64(v) => Some(Arc::new((v + 1).into())),
            Val::U64(v) => Some(Arc::new((v + 1).into())),
            Val::Char(_) => panic!(),
            Val::String(_) => panic!(),
            Val::Cid(_) => panic!(),
        }
    }
}

impl AggregationFn for Sum {
    fn start(&self, target_type: Type) -> Option<Arc<Val>> {
        target_type.zero().map(Arc::new)
    }

    fn apply(&self, cur: Option<Arc<Val>>, new: Arc<Val>) -> Option<Arc<Val>> {
        match (&*cur.unwrap(), &*new) {
            (Val::S8(v1), Val::S8(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::U8(v1), Val::U8(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::S16(v1), Val::S16(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::U16(v1), Val::U16(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::S32(v1), Val::S32(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::U32(v1), Val::U32(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::S64(v1), Val::S64(v2)) => Some(Arc::new((v1 + v2).into())),
            (Val::U64(v1), Val::U64(v2)) => Some(Arc::new((v1 + v2).into())),
            _ => panic!(),
        }
    }
}

impl AggregationFn for Min {
    fn start(&self, _: Type) -> Option<Arc<Val>> {
        None
    }

    fn apply(&self, cur: Option<Arc<Val>>, new: Arc<Val>) -> Option<Arc<Val>> {
        if cur.is_none() {
            return Some(new);
        }

        match (&*cur.unwrap(), &*new) {
            (Val::S8(v1), Val::S8(v2)) => Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into())),
            (Val::U8(v1), Val::U8(v2)) => Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into())),
            (Val::S16(v1), Val::S16(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U16(v1), Val::U16(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::S32(v1), Val::S32(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U32(v1), Val::U32(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::S64(v1), Val::S64(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U64(v1), Val::U64(v2)) => {
                Some(Arc::new(min_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            _ => panic!(),
        }
    }
}

impl AggregationFn for Max {
    fn start(&self, _: Type) -> Option<Arc<Val>> {
        None
    }

    fn apply(&self, cur: Option<Arc<Val>>, new: Arc<Val>) -> Option<Arc<Val>> {
        if cur.is_none() {
            return Some(new);
        }

        match (&*cur.unwrap(), &*new) {
            (Val::S8(v1), Val::S8(v2)) => Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into())),
            (Val::U8(v1), Val::U8(v2)) => Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into())),
            (Val::S16(v1), Val::S16(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U16(v1), Val::U16(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::S32(v1), Val::S32(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U32(v1), Val::U32(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::S64(v1), Val::S64(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            (Val::U64(v1), Val::U64(v2)) => {
                Some(Arc::new(max_by(*v1, *v2, |x, y| x.cmp(y)).into()))
            }
            _ => panic!(),
        }
    }
}

impl Display for AggregationFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregationFunction::Count(_) => f.write_str("count"),
            AggregationFunction::Sum(_) => f.write_str("sum"),
            AggregationFunction::Min(_) => f.write_str("min"),
            AggregationFunction::Max(_) => f.write_str("max"),
        }
    }
}

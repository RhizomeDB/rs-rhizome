use std::{fmt, marker::PhantomData};

use crate::{
    predicate::{Predicate, PredicateWhere},
    typed_vars::TypedVars,
    var::Var,
};

pub mod math;

pub fn when<F, V, I>(args: V, f: F) -> FnPredicate<F, V, I>
where
    V: TypedVars<Args = I>,
    F: Fn(I) -> bool + Send + Sync + 'static,
{
    FnPredicate(f, args, PhantomData)
}

pub struct FnPredicate<F, V, I>(F, V, PhantomData<I>)
where
    V: TypedVars<Args = I>,
    F: Fn(I) -> bool + Send + Sync + 'static;

impl<F, V, I> Predicate for FnPredicate<F, V, I>
where
    V: TypedVars<Args = I>,
    F: Fn(I) -> bool + Send + Sync + 'static,
{
    type Input = I;

    fn apply(&self, args: Self::Input) -> Option<bool> {
        Some(self.0(args))
    }
}

impl<F, V, I> PredicateWhere<I> for FnPredicate<F, V, I>
where
    V: TypedVars<Args = I>,
    F: Fn(I) -> bool + Send + Sync + 'static,
{
    type Predicate = Self;

    fn into_predicate(self) -> Self::Predicate {
        self
    }

    fn as_args(&self) -> Vec<Var> {
        self.1.vars()
    }
}

impl<F, V, I> fmt::Debug for FnPredicate<F, V, I>
where
    V: TypedVars<Args = I>,
    F: Fn(I) -> bool + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FnPredicate").finish()
    }
}

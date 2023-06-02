use crate::{args::Args, value::Val, var::Var};

pub trait PredicateWhere<I> {
    type Predicate: Predicate<Input = I>;

    fn into_predicate(self) -> Self::Predicate;

    // TODO: support arbitrary expressions here, instead of just vars
    fn as_args(&self) -> Vec<Var>;
}

pub trait Predicate: Sized {
    type Input;

    // TODO: Make this Result<bool, E> with generic E?
    fn apply(&self, args: Self::Input) -> Option<bool>;
}

impl<T: ?Sized> Predicate for Box<T>
where
    T: Predicate,
{
    type Input = T::Input;

    fn apply(&self, args: Self::Input) -> Option<bool> {
        (**self).apply(args)
    }
}

pub trait PredicateWrapper: Send + Sync + 'static {
    fn apply(&self, args: Vec<Val>) -> Option<bool>;
}

impl<T, I> PredicateWrapper for T
where
    T: Predicate<Input = I> + Send + Sync + 'static,
    I: Args,
{
    fn apply(&self, args: Vec<Val>) -> Option<bool> {
        let args = <T::Input as Args>::instantiate(args).unwrap();

        T::apply(self, args)
    }
}

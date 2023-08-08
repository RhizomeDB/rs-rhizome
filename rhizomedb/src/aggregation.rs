use crate::{args::Args, types::IntoColType, value::Val, var::Var};

pub trait AggAcc:
    IntoColType + Into<Val> + TryFrom<Val, Error = ()> + Send + Sync + 'static
{
}

impl<T> AggAcc for T
where
    T: IntoColType + Into<Val> + TryFrom<Val, Error = ()>,
    T: Send + Sync + 'static,
{
}

pub trait AggregateGroupBy<I, O> {
    type Aggregate: Aggregate<Input = I, Output = O>;

    // TODO: support arbitrary expressions here, instead of just vars
    fn as_args(&self) -> Vec<Var>;
}

pub trait Aggregate: Sized + Default {
    type Input;
    type Output;

    fn step(&mut self, args: Self::Input);
    fn finalize(&self) -> Option<Self::Output>;
}

impl<T: ?Sized> Aggregate for Box<T>
where
    T: Aggregate,
{
    type Input = T::Input;
    type Output = T::Output;

    fn step(&mut self, args: Self::Input) {
        (**self).step(args);
    }

    fn finalize(&self) -> Option<Self::Output> {
        T::finalize(self)
    }
}

pub trait AggregateWrapper: Send + Sync + 'static {
    fn init(&self) -> Box<dyn AggregateWrapper>;
    fn step(&mut self, args: Vec<Val>);
    fn finalize(&self) -> Option<Val>;
}

impl<T, I, O> AggregateWrapper for T
where
    T: Aggregate<Input = I, Output = O> + Send + Sync + 'static,
    I: Args,
    O: AggAcc,
{
    fn init(&self) -> Box<dyn AggregateWrapper> {
        Box::<T>::default()
    }

    fn step(&mut self, args: Vec<Val>) {
        let args = <T::Input as Args>::instantiate(args).unwrap();

        T::step(self, args);
    }

    fn finalize(&self) -> Option<Val> {
        T::finalize(self).map(Into::into)
    }
}

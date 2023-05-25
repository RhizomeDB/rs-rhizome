use crate::var::TypedVar;
use crate::{types::IntoColType, value::Val, var::Var};

use anyhow::Result;

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

pub trait AggArgs: Sized + Send + Sync + 'static {
    type Vars;

    fn vars(vars: Self::Vars) -> Vec<Var>;
    fn instantiate(bindings: Vec<Val>) -> Result<Self, ()>;
}

impl AggArgs for () {
    type Vars = ();

    fn vars(_vars: Self::Vars) -> Vec<Var> {
        vec![]
    }
    fn instantiate(_bindings: Vec<Val>) -> Result<Self, ()> {
        Ok(())
    }
}

impl<T> AggArgs for (T,)
where
    T: IntoColType + TryFrom<Val, Error = ()> + Send + Sync + 'static,
{
    type Vars = TypedVar<T>;

    fn vars(vars: Self::Vars) -> Vec<Var> {
        vec![vars.as_var()]
    }

    fn instantiate(bindings: Vec<Val>) -> Result<Self, ()> {
        bindings[0].clone().try_into().map(|t| (t,))
    }
}

macro_rules! impl_agg_args {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<$([< T $Ts >],)*> AggArgs for ($([< T $Ts >],)*)
            where
                $(
                    [< T $Ts >]: IntoColType + TryFrom<Val, Error = ()> + Send + Sync + 'static,
                )*
            {
                type Vars = ($(TypedVar<[< T $Ts >]>,)*);

                fn vars(vars: Self::Vars) -> Vec<Var> {
                     vec![$(vars.$Ts.as_var(),)*]
                }

                #[allow(unused_variables)]
                #[allow(clippy::unused_unit)]
                fn instantiate(bindings: Vec<Val>) -> Result<Self, ()> {
                    Ok((
                        $(
                            bindings[$Ts].clone().try_into()?,
                        )*
                    ))
                }
            }
        }
    };
}

impl_agg_args!(0, 1);
impl_agg_args!(0, 1, 2);
impl_agg_args!(0, 1, 2, 3);
impl_agg_args!(0, 1, 2, 3, 4);
impl_agg_args!(0, 1, 2, 3, 4, 5);
impl_agg_args!(0, 1, 2, 3, 4, 5, 6);
impl_agg_args!(0, 1, 2, 3, 4, 5, 6, 7);

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

impl<T, Args, Acc> AggregateWrapper for T
where
    T: Aggregate<Input = Args, Output = Acc> + Send + Sync + 'static,
    Args: AggArgs,
    Acc: AggAcc,
{
    fn init(&self) -> Box<dyn AggregateWrapper> {
        Box::<T>::default()
    }

    fn step(&mut self, args: Vec<Val>) {
        let args = <T::Input as AggArgs>::instantiate(args).unwrap();

        T::step(self, args);
    }

    fn finalize(&self) -> Option<Val> {
        T::finalize(self).map(Into::into)
    }
}

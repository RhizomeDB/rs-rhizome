use crate::{
    types::{FromType, Type},
    var::{TypedVar, Var},
};

pub trait VarRefTuple<O> {
    type Target;

    fn deref(&self) -> Self::Target;
}

macro_rules! impl_var_ref_tuple {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<'a, O, $([< V $Ts >],)*> VarRefTuple<O> for ($(&'a TypedVar<[< V $Ts >]>,)*)
            where
                O: Clone,
                $(
                    Type: FromType<[< V $Ts >]>,
                    [< V $Ts >]: Copy + TryFrom<O, Error = &'static str>,
                )*
            {
                type Target = ($(TypedVar<[< V $Ts >]>,)*);

                fn deref(&self) -> Self::Target {
                    ($(*self.$Ts,)*)
                }
            }
        }
    };
}

pub trait IntoTupleArgs<O> {
    type Output;

    fn into_vars(&self) -> Vec<Var>;
    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output;
}

macro_rules! impl_into_tuple_args {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<O, $([< V $Ts >],)*> IntoTupleArgs<O> for ($(TypedVar<[< V $Ts >]>,)*)
            where
                O: Clone,
                $(
                    Type: FromType<[< V $Ts >]>,
                    [< V $Ts >]: Copy + TryFrom<O, Error = &'static str>,
                )*
            {
                type Output = ($([< V $Ts >],)*);

                fn into_vars(&self) -> Vec<Var> {
                     vec![$(self.$Ts.into(),)*]
                }

                #[allow(unused_variables)]
                fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
                    (
                        $(
                           [< V $Ts >]::try_from(bindings[$Ts].clone()).unwrap(),
                        )*
                    )
                }
            }
        }
    };
}

impl_into_tuple_args!();
impl_into_tuple_args!(0);
impl_into_tuple_args!(0, 1);
impl_into_tuple_args!(0, 1, 2);
impl_into_tuple_args!(0, 1, 2, 3);
impl_into_tuple_args!(0, 1, 2, 3, 4);
impl_into_tuple_args!(0, 1, 2, 3, 4, 5);
impl_into_tuple_args!(0, 1, 2, 3, 4, 5, 6);
impl_into_tuple_args!(0, 1, 2, 3, 4, 5, 6, 7);

impl_var_ref_tuple!();
impl_var_ref_tuple!(0);
impl_var_ref_tuple!(0, 1);
impl_var_ref_tuple!(0, 1, 2);
impl_var_ref_tuple!(0, 1, 2, 3);
impl_var_ref_tuple!(0, 1, 2, 3, 4);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5, 6);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5, 6, 7);

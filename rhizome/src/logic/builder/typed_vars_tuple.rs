use anyhow::Result;

use crate::{
    types::{ColType, FromType, Type},
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
                    ColType: FromType<[< V $Ts >]>,
                    [< V $Ts >]: Copy + TryFrom<O, Error = &'static str>,
                )*
            {
                type Target = ($(TypedVar<[< V $Ts >]>,)*);

                #[allow(clippy::unused_unit)]
                fn deref(&self) -> Self::Target {
                    ($(*self.$Ts,)*)
                }
            }
        }
    };
}

pub trait TypedVarsTuple<O> {
    type Output;

    fn vars(&self) -> Vec<Var>;
    fn args(&self, bindings: Vec<O>) -> Result<Self::Output>;
}

macro_rules! impl_typed_vars_tuple {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<O, $([< V $Ts >],)*> TypedVarsTuple<O> for ($(TypedVar<[< V $Ts >]>,)*)
            where
                O: Clone,
                $(
                    Type: FromType<[< V $Ts >]>,
                    [< V $Ts >]: Copy + TryFrom<O, Error = &'static str>,
                )*
            {
                type Output = ($([< V $Ts >],)*);

                fn vars(&self) -> Vec<Var> {
                     vec![$(self.$Ts.into(),)*]
                }

                #[allow(unused_variables)]
                #[allow(clippy::unused_unit)]
                fn args(&self, bindings: Vec<O>) -> Result<Self::Output> {
                    Ok((
                        $(
                            [< V $Ts >]::try_from(bindings[$Ts].clone()).map_err(|_| $crate::error::Error::InternalRhizomeError("too few runtime args passed".to_owned()))?,
                        )*
                    ))
                }
            }
        }
    };
}

impl_typed_vars_tuple!();
impl_typed_vars_tuple!(0);
impl_typed_vars_tuple!(0, 1);
impl_typed_vars_tuple!(0, 1, 2);
impl_typed_vars_tuple!(0, 1, 2, 3);
impl_typed_vars_tuple!(0, 1, 2, 3, 4);
impl_typed_vars_tuple!(0, 1, 2, 3, 4, 5);
impl_typed_vars_tuple!(0, 1, 2, 3, 4, 5, 6);
impl_typed_vars_tuple!(0, 1, 2, 3, 4, 5, 6, 7);

impl_var_ref_tuple!();
impl_var_ref_tuple!(0);
impl_var_ref_tuple!(0, 1);
impl_var_ref_tuple!(0, 1, 2);
impl_var_ref_tuple!(0, 1, 2, 3);
impl_var_ref_tuple!(0, 1, 2, 3, 4);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5, 6);
impl_var_ref_tuple!(0, 1, 2, 3, 4, 5, 6, 7);

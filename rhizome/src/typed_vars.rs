use anyhow::Result;

use crate::{
    types::IntoColType,
    value::Val,
    var::{TypedVar, Var},
};

pub trait TypedVars {
    type Args;

    fn vars(&self) -> Vec<Var>;

    // TODO: return an InternalRhizomeError instead of ()
    #[allow(clippy::result_unit_err)]
    fn args(&self, bindings: Vec<Val>) -> Result<Self::Args, ()>;
}

impl TypedVars for () {
    type Args = ();

    fn vars(&self) -> Vec<Var> {
        vec![]
    }

    fn args(&self, _bindings: Vec<Val>) -> Result<Self::Args, ()> {
        Ok(())
    }
}

impl<V> TypedVars for TypedVar<V>
where
    V: IntoColType + TryFrom<Val, Error = ()>,
{
    type Args = V;

    fn vars(&self) -> Vec<Var> {
        vec![self.as_var()]
    }

    fn args(&self, bindings: Vec<Val>) -> Result<Self::Args, ()> {
        bindings[0].clone().try_into()
    }
}

macro_rules! impl_typed_vars {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<$([< V $Ts >],)*> TypedVars for ($(TypedVar<[< V $Ts >]>,)*)
            where
                $(
                    [< V $Ts >]: IntoColType + TryFrom<Val, Error = ()>,
                )*
            {
                type Args = ($([< V $Ts >],)*);

                fn vars(&self) -> Vec<Var> {
                     vec![$(self.$Ts.as_var(),)*]
                }

                #[allow(unused_variables)]
                #[allow(clippy::unused_unit)]
                fn args(&self, bindings: Vec<Val>) -> Result<Self::Args, ()> {
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

impl_typed_vars!(0);
impl_typed_vars!(0, 1);
impl_typed_vars!(0, 1, 2);
impl_typed_vars!(0, 1, 2, 3);
impl_typed_vars!(0, 1, 2, 3, 4);
impl_typed_vars!(0, 1, 2, 3, 4, 5);
impl_typed_vars!(0, 1, 2, 3, 4, 5, 6);
impl_typed_vars!(0, 1, 2, 3, 4, 5, 6, 7);

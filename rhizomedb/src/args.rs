use crate::{types::IntoColType, value::Val};

pub trait Args: Sized + Send + Sync + 'static {
    // TODO: return an InternalRhizomeError instead of ()
    #[allow(clippy::result_unit_err)]
    fn instantiate(bindings: Vec<Val>) -> Result<Self, ()>;
}

impl Args for () {
    fn instantiate(_bindings: Vec<Val>) -> Result<Self, ()> {
        Ok(())
    }
}

macro_rules! impl_args {
    ($($Ts:expr),*) => {
        paste::item! {
            impl<$([< T $Ts >],)*> Args for ($([< T $Ts >],)*)
            where
                $(
                    [< T $Ts >]: IntoColType + TryFrom<Val, Error = ()> + Send + Sync + 'static,
                )*
            {
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

impl_args!(0);
impl_args!(0, 1);
impl_args!(0, 1, 2);
impl_args!(0, 1, 2, 3);
impl_args!(0, 1, 2, 3, 4);
impl_args!(0, 1, 2, 3, 4, 5);
impl_args!(0, 1, 2, 3, 4, 5, 6);
impl_args!(0, 1, 2, 3, 4, 5, 6, 7);

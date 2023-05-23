use crate::{
    types::{ColType, FromType, Type},
    var::TypedVar,
};

pub trait RuleVars {
    type Vars;

    fn into_vars(idx: usize) -> Self::Vars;
}

impl<V0> RuleVars for V0
where
    Type: FromType<V0>,
{
    type Vars = TypedVar<V0>;

    fn into_vars(idx: usize) -> Self::Vars {
        TypedVar::<V0>::new(format!("x{idx}").as_ref())
    }
}

macro_rules! impl_rule_vars_for_array {
    ($n:expr) => {
        paste::item! {
            impl<T> RuleVars for [T; $n]
            where
            ColType: FromType<T>,
            {
                type Vars = [TypedVar<T>; $n];

                fn into_vars(idx: usize) -> Self::Vars {
                    core::array::from_fn(|i|
                        TypedVar::<T>::new(format!("x{}", idx + i).as_ref())
                    )
                }
            }
        }
    };
}

macro_rules! impl_rule_vars_for_tuple {
    ($($Vs:expr),*) => {
        paste::item! {
            impl<$([< V $Vs >],)*> RuleVars for ($([< V $Vs >],)*)
            where
            $(Type: FromType<[< V $Vs >]>,)*
            {
                type Vars = ($(TypedVar<[< V $Vs >]>,)*);

                #[allow(clippy::unused_unit, unused_variables)]
                fn into_vars(idx: usize) -> Self::Vars {
                    ($(TypedVar::<[< V $Vs >]>::new(format!("x{}", idx + [< $Vs >]).as_ref()),)*)
                }
            }
        }
    };
}

impl_rule_vars_for_array!(0);
impl_rule_vars_for_array!(1);
impl_rule_vars_for_array!(2);
impl_rule_vars_for_array!(3);
impl_rule_vars_for_array!(4);
impl_rule_vars_for_array!(5);
impl_rule_vars_for_array!(6);
impl_rule_vars_for_array!(7);
impl_rule_vars_for_array!(8);
impl_rule_vars_for_array!(9);
impl_rule_vars_for_array!(10);
impl_rule_vars_for_array!(11);
impl_rule_vars_for_array!(12);

impl_rule_vars_for_tuple!();
impl_rule_vars_for_tuple!(0);
impl_rule_vars_for_tuple!(0, 1);
impl_rule_vars_for_tuple!(0, 1, 2);
impl_rule_vars_for_tuple!(0, 1, 2, 3);
impl_rule_vars_for_tuple!(0, 1, 2, 3, 4);
impl_rule_vars_for_tuple!(0, 1, 2, 3, 4, 5);
impl_rule_vars_for_tuple!(0, 1, 2, 3, 4, 5, 6);
impl_rule_vars_for_tuple!(0, 1, 2, 3, 4, 5, 6, 7);

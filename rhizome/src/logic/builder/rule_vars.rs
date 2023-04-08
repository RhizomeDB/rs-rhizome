use crate::{
    types::{ColType, FromType, Type},
    var::TypedVar,
};

pub trait RuleVars {
    type Vars;

    fn into_vars(idx: usize) -> Self::Vars;
}

impl<T> RuleVars for [T; 0]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 0];

    fn into_vars(_idx: usize) -> Self::Vars {
        []
    }
}

impl<T> RuleVars for [T; 1]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 1];

    fn into_vars(idx: usize) -> Self::Vars {
        [0].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 2]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 2];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 3]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 3];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 4]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 4];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 5]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 5];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3, 4].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 6]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 6];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3, 4, 5].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 7]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 7];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3, 4, 5, 6].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 8]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 8];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3, 4, 5, 6, 7].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl<T> RuleVars for [T; 9]
where
    ColType: FromType<T>,
{
    type Vars = [TypedVar<T>; 9];

    fn into_vars(idx: usize) -> Self::Vars {
        [0, 1, 2, 3, 4, 5, 6, 7, 8].map(|i| TypedVar::<T>::new(format!("x{}", idx + i).as_ref()))
    }
}

impl RuleVars for () {
    type Vars = ();

    fn into_vars(_idx: usize) -> Self::Vars {}
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

impl<V0> RuleVars for (V0,)
where
    Type: FromType<V0>,
{
    type Vars = (TypedVar<V0>,);

    fn into_vars(idx: usize) -> Self::Vars {
        (TypedVar::<V0>::new(format!("x{idx}").as_ref()),)
    }
}

impl<V0, V1> RuleVars for (V0, V1)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    type Vars = (TypedVar<V0>, TypedVar<V1>);

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
        )
    }
}

impl<V0, V1, V2> RuleVars for (V0, V1, V2)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    type Vars = (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>);

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3> RuleVars for (V0, V1, V2, V3)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    type Vars = (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>, TypedVar<V3>);

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4> RuleVars for (V0, V1, V2, V3, V4)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5> RuleVars for (V0, V1, V2, V3, V4, V5)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
        TypedVar<V5>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
            TypedVar::<V5>::new(format!("x{}", idx + 5).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6> RuleVars for (V0, V1, V2, V3, V4, V5, V6)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
        TypedVar<V5>,
        TypedVar<V6>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
            TypedVar::<V5>::new(format!("x{}", idx + 5).as_ref()),
            TypedVar::<V6>::new(format!("x{}", idx + 6).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
        TypedVar<V5>,
        TypedVar<V6>,
        TypedVar<V7>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
            TypedVar::<V5>::new(format!("x{}", idx + 5).as_ref()),
            TypedVar::<V6>::new(format!("x{}", idx + 6).as_ref()),
            TypedVar::<V7>::new(format!("x{}", idx + 7).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7, V8> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
    Type: FromType<V8>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
        TypedVar<V5>,
        TypedVar<V6>,
        TypedVar<V7>,
        TypedVar<V8>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
            TypedVar::<V5>::new(format!("x{}", idx + 5).as_ref()),
            TypedVar::<V6>::new(format!("x{}", idx + 6).as_ref()),
            TypedVar::<V7>::new(format!("x{}", idx + 7).as_ref()),
            TypedVar::<V8>::new(format!("x{}", idx + 8).as_ref()),
        )
    }
}

impl<V0, V1, V2, V3, V4, V5, V6, V7, V8, V9> RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8, V9)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
    Type: FromType<V6>,
    Type: FromType<V7>,
    Type: FromType<V8>,
    Type: FromType<V9>,
{
    type Vars = (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
        TypedVar<V5>,
        TypedVar<V6>,
        TypedVar<V7>,
        TypedVar<V8>,
        TypedVar<V9>,
    );

    fn into_vars(idx: usize) -> Self::Vars {
        (
            TypedVar::<V0>::new(format!("x{idx}").as_ref()),
            TypedVar::<V1>::new(format!("x{}", idx + 1).as_ref()),
            TypedVar::<V2>::new(format!("x{}", idx + 2).as_ref()),
            TypedVar::<V3>::new(format!("x{}", idx + 3).as_ref()),
            TypedVar::<V4>::new(format!("x{}", idx + 4).as_ref()),
            TypedVar::<V5>::new(format!("x{}", idx + 5).as_ref()),
            TypedVar::<V6>::new(format!("x{}", idx + 6).as_ref()),
            TypedVar::<V7>::new(format!("x{}", idx + 7).as_ref()),
            TypedVar::<V8>::new(format!("x{}", idx + 8).as_ref()),
            TypedVar::<V9>::new(format!("x{}", idx + 9).as_ref()),
        )
    }
}

use crate::{
    types::{FromType, Type},
    var::TypedVar,
};

pub trait RuleVars {
    type Vars;

    fn into_vars() -> Self::Vars;
}

impl RuleVars for () {
    type Vars = ();

    fn into_vars() -> Self::Vars {}
}

impl<V0> RuleVars for (V0,)
where
    Type: FromType<V0>,
{
    type Vars = (TypedVar<V0>,);

    fn into_vars() -> Self::Vars {
        (TypedVar::<V0>::new("x0"),)
    }
}

impl<V0, V1> RuleVars for (V0, V1)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    type Vars = (TypedVar<V0>, TypedVar<V1>);

    fn into_vars() -> Self::Vars {
        (TypedVar::<V0>::new("x0"), TypedVar::<V1>::new("x1"))
    }
}

impl<V0, V1, V2> RuleVars for (V0, V1, V2)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    type Vars = (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>);

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
            TypedVar::<V5>::new("x5"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
            TypedVar::<V5>::new("x5"),
            TypedVar::<V6>::new("x6"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
            TypedVar::<V5>::new("x5"),
            TypedVar::<V6>::new("x6"),
            TypedVar::<V7>::new("x7"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
            TypedVar::<V5>::new("x5"),
            TypedVar::<V6>::new("x6"),
            TypedVar::<V7>::new("x7"),
            TypedVar::<V8>::new("x8"),
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

    fn into_vars() -> Self::Vars {
        (
            TypedVar::<V0>::new("x0"),
            TypedVar::<V1>::new("x1"),
            TypedVar::<V2>::new("x2"),
            TypedVar::<V3>::new("x3"),
            TypedVar::<V4>::new("x4"),
            TypedVar::<V5>::new("x5"),
            TypedVar::<V6>::new("x6"),
            TypedVar::<V7>::new("x7"),
            TypedVar::<V8>::new("x8"),
            TypedVar::<V9>::new("x9"),
        )
    }
}

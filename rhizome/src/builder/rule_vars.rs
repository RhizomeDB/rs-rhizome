use crate::{
    logic::ast::Var,
    types::{FromType, Type},
};

// TODO: Implement this with macros

pub trait RuleVars {
    type Vars;

    fn into_vars() -> Self::Vars;
}

impl RuleVars for () {
    type Vars = ();

    fn into_vars() -> Self::Vars {
        
    }
}

impl<V0> RuleVars for (V0,)
where
    Type: FromType<V0>,
{
    type Vars = (Var,);

    fn into_vars() -> Self::Vars {
        (Var::new::<V0>("x0"),)
    }
}

impl<V0, V1> RuleVars for (V0, V1)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    type Vars = (Var, Var);

    fn into_vars() -> Self::Vars {
        (Var::new::<V0>("x0"), Var::new::<V1>("x1"))
    }
}

impl<V0, V1, V2> RuleVars for (V0, V1, V2)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    type Vars = (Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
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
    type Vars = (Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
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
    type Vars = (Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
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
    type Vars = (Var, Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
            Var::new::<V5>("x5"),
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
    type Vars = (Var, Var, Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
            Var::new::<V5>("x5"),
            Var::new::<V6>("x6"),
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
    type Vars = (Var, Var, Var, Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
            Var::new::<V5>("x5"),
            Var::new::<V6>("x6"),
            Var::new::<V7>("x7"),
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
    type Vars = (Var, Var, Var, Var, Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
            Var::new::<V5>("x5"),
            Var::new::<V6>("x6"),
            Var::new::<V7>("x7"),
            Var::new::<V8>("x8"),
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
    type Vars = (Var, Var, Var, Var, Var, Var, Var, Var, Var, Var);

    fn into_vars() -> Self::Vars {
        (
            Var::new::<V0>("x0"),
            Var::new::<V1>("x1"),
            Var::new::<V2>("x2"),
            Var::new::<V3>("x3"),
            Var::new::<V4>("x4"),
            Var::new::<V5>("x5"),
            Var::new::<V6>("x6"),
            Var::new::<V7>("x7"),
            Var::new::<V8>("x8"),
            Var::new::<V9>("x9"),
        )
    }
}

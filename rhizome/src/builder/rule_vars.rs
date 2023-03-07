use crate::{
    logic::ast::Var,
    types::{FromType, Type},
};

// TODO: Implement this with macros

pub trait RuleVars {
    type Vars: 'static;
    const V: &'static Self::Vars;
}

impl RuleVars for () {
    type Vars = ();

    const V: &'static Self::Vars = &();
}

impl<V0: 'static> RuleVars for (V0,)
where
    Type: FromType<V0>,
{
    type Vars = (Var,);

    const V: &'static Self::Vars = &(Var::new(0),);
}

impl<V0: 'static, V1: 'static> RuleVars for (V0, V1)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    type Vars = (Var, Var);

    const V: &'static Self::Vars = &(Var::new(0), Var::new(1));
}

impl<V0: 'static, V1: 'static, V2: 'static> RuleVars for (V0, V1, V2)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    type Vars = (Var, Var, Var);

    const V: &'static Self::Vars = &(Var::new(0), Var::new(1), Var::new(2));
}

impl<V0: 'static, V1: 'static, V2: 'static, V3: 'static> RuleVars for (V0, V1, V2, V3)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    type Vars = (Var, Var, Var, Var);

    const V: &'static Self::Vars = &(Var::new(0), Var::new(1), Var::new(2), Var::new(3));
}

impl<V0: 'static, V1: 'static, V2: 'static, V3: 'static, V4: 'static> RuleVars
    for (V0, V1, V2, V3, V4)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
{
    type Vars = (Var, Var, Var, Var, Var);

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
    );
}

impl<V0: 'static, V1: 'static, V2: 'static, V3: 'static, V4: 'static, V5: 'static> RuleVars
    for (V0, V1, V2, V3, V4, V5)
where
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    Type: FromType<V5>,
{
    type Vars = (Var, Var, Var, Var, Var, Var);

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
        Var::new(5),
    );
}

impl<V0: 'static, V1: 'static, V2: 'static, V3: 'static, V4: 'static, V5: 'static, V6: 'static>
    RuleVars for (V0, V1, V2, V3, V4, V5, V6)
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

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
        Var::new(5),
        Var::new(6),
    );
}

impl<
        V0: 'static,
        V1: 'static,
        V2: 'static,
        V3: 'static,
        V4: 'static,
        V5: 'static,
        V6: 'static,
        V7: 'static,
    > RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7)
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

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
        Var::new(5),
        Var::new(6),
        Var::new(7),
    );
}

impl<
        V0: 'static,
        V1: 'static,
        V2: 'static,
        V3: 'static,
        V4: 'static,
        V5: 'static,
        V6: 'static,
        V7: 'static,
        V8: 'static,
    > RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8)
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

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
        Var::new(5),
        Var::new(6),
        Var::new(7),
        Var::new(8),
    );
}

impl<
        V0: 'static,
        V1: 'static,
        V2: 'static,
        V3: 'static,
        V4: 'static,
        V5: 'static,
        V6: 'static,
        V7: 'static,
        V8: 'static,
        V9: 'static,
    > RuleVars for (V0, V1, V2, V3, V4, V5, V6, V7, V8, V9)
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

    const V: &'static Self::Vars = &(
        Var::new(0),
        Var::new(1),
        Var::new(2),
        Var::new(3),
        Var::new(4),
        Var::new(5),
        Var::new(6),
        Var::new(7),
        Var::new(8),
        Var::new(9),
    );
}

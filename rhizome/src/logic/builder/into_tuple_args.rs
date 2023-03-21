use crate::{
    types::{FromType, Type},
    var::{TypedVar, Var},
};

pub trait VarRefTuple<O> {
    type Target: IntoTupleArgs<O>;
    fn deref(&self) -> Self::Target;
}

impl<O> VarRefTuple<O> for () {
    type Target = ();

    fn deref(&self) -> Self::Target {}
}

impl<'a, O, V0> VarRefTuple<O> for (&'a TypedVar<V0>,)
where
    O: Clone,
    Type: FromType<V0>,
    V0: Copy + TryFrom<O, Error = &'static str>,
{
    type Target = (TypedVar<V0>,);

    fn deref(&self) -> Self::Target {
        (*self.0,)
    }
}

impl<'a, O, V0, V1> VarRefTuple<O> for (&'a TypedVar<V0>, &'a TypedVar<V1>)
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
{
    type Target = (TypedVar<V0>, TypedVar<V1>);

    fn deref(&self) -> Self::Target {
        (*self.0, *self.1)
    }
}

impl<'a, O, V0, V1, V2> VarRefTuple<O> for (&'a TypedVar<V0>, &'a TypedVar<V1>, &'a TypedVar<V2>)
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
    V2: Copy + TryFrom<O, Error = &'static str>,
{
    type Target = (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>);

    fn deref(&self) -> Self::Target {
        (*self.0, *self.1, *self.2)
    }
}

impl<'a, O, V0, V1, V2, V3> VarRefTuple<O>
    for (
        &'a TypedVar<V0>,
        &'a TypedVar<V1>,
        &'a TypedVar<V2>,
        &'a TypedVar<V3>,
    )
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
    V2: Copy + TryFrom<O, Error = &'static str>,
    V3: Copy + TryFrom<O, Error = &'static str>,
{
    type Target = (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>, TypedVar<V3>);

    fn deref(&self) -> Self::Target {
        (*self.0, *self.1, *self.2, *self.3)
    }
}

pub trait IntoTupleArgs<O> {
    type Output;

    fn into_vars(&self) -> Vec<Var>;
    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output;
}

impl<O> IntoTupleArgs<O> for () {
    type Output = ();

    fn into_vars(&self) -> Vec<Var> {
        vec![]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.is_empty());
    }
}

impl<V0, O> IntoTupleArgs<O> for (TypedVar<V0>,)
where
    O: Clone,
    Type: FromType<V0>,
    V0: Copy + TryFrom<O, Error = &'static str>,
{
    type Output = (V0,);

    fn into_vars(&self) -> Vec<Var> {
        vec![self.0.into()]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.len() == 1);

        (V0::try_from(bindings[0].clone()).unwrap(),)
    }
}

impl<V0, V1, O> IntoTupleArgs<O> for (TypedVar<V0>, TypedVar<V1>)
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
{
    type Output = (V0, V1);
    fn into_vars(&self) -> Vec<Var> {
        vec![self.0.into(), self.1.into()]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.len() == 2);

        (
            V0::try_from(bindings[0].clone()).unwrap(),
            V1::try_from(bindings[1].clone()).unwrap(),
        )
    }
}
impl<V0, V1, V2, O> IntoTupleArgs<O> for (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>)
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
    V2: Copy + TryFrom<O, Error = &'static str>,
{
    type Output = (V0, V1, V2);

    fn into_vars(&self) -> Vec<Var> {
        vec![self.0.into(), self.1.into(), self.2.into()]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.len() == 3);

        (
            V0::try_from(bindings[0].clone()).unwrap(),
            V1::try_from(bindings[1].clone()).unwrap(),
            V2::try_from(bindings[2].clone()).unwrap(),
        )
    }
}

impl<V0, V1, V2, V3, O> IntoTupleArgs<O>
    for (TypedVar<V0>, TypedVar<V1>, TypedVar<V2>, TypedVar<V3>)
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
    V2: Copy + TryFrom<O, Error = &'static str>,
    V3: Copy + TryFrom<O, Error = &'static str>,
{
    type Output = (V0, V1, V2, V3);

    fn into_vars(&self) -> Vec<Var> {
        vec![self.0.into(), self.1.into(), self.2.into(), self.3.into()]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.len() == 4);

        (
            V0::try_from(bindings[0].clone()).unwrap(),
            V1::try_from(bindings[1].clone()).unwrap(),
            V2::try_from(bindings[2].clone()).unwrap(),
            V3::try_from(bindings[3].clone()).unwrap(),
        )
    }
}

impl<V0, V1, V2, V3, V4, O> IntoTupleArgs<O>
    for (
        TypedVar<V0>,
        TypedVar<V1>,
        TypedVar<V2>,
        TypedVar<V3>,
        TypedVar<V4>,
    )
where
    O: Clone,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
    Type: FromType<V4>,
    V0: Copy + TryFrom<O, Error = &'static str>,
    V1: Copy + TryFrom<O, Error = &'static str>,
    V2: Copy + TryFrom<O, Error = &'static str>,
    V3: Copy + TryFrom<O, Error = &'static str>,
    V4: Copy + TryFrom<O, Error = &'static str>,
{
    type Output = (V0, V1, V2, V3, V4);
    fn into_vars(&self) -> Vec<Var> {
        vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
        ]
    }

    fn into_tuple_args(&self, bindings: Vec<O>) -> Self::Output {
        assert!(bindings.len() == 5);

        (
            V0::try_from(bindings[0].clone()).unwrap(),
            V1::try_from(bindings[1].clone()).unwrap(),
            V2::try_from(bindings[2].clone()).unwrap(),
            V3::try_from(bindings[3].clone()).unwrap(),
            V4::try_from(bindings[4].clone()).unwrap(),
        )
    }
}

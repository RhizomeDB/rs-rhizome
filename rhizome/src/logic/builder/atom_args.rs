use crate::{
    col_val::ColVal,
    id::ColId,
    types::{FromType, Type},
    value::Val,
    var::TypedVar,
};

pub trait TransitiveInto<Via, To> {
    fn into_transitive(self) -> To;
}

impl<From, Via, To> TransitiveInto<Via, To> for From
where
    From: Into<Via>,
    Via: Into<To>,
{
    fn into_transitive(self) -> To {
        let via: Via = self.into();

        via.into()
    }
}

pub trait AtomArg<T> {
    fn into_col(self) -> (ColId, ColVal);
}

impl<'a, T> AtomArg<Val> for (&'a str, T)
where
    T: TransitiveInto<Val, ColVal>,
{
    fn into_col(self) -> (ColId, ColVal) {
        (ColId::new(self.0), self.1.into_transitive())
    }
}

impl<'a, V, T> AtomArg<&'a TypedVar<V>> for (&'a str, T)
where
    T: TransitiveInto<&'a TypedVar<V>, ColVal>,
    Type: FromType<V>,
{
    fn into_col(self) -> (ColId, ColVal) {
        (ColId::new(self.0), self.1.into_transitive())
    }
}

pub trait AtomArgs<T> {
    fn into_cols(self) -> Vec<(ColId, ColVal)>;
}

impl AtomArgs<()> for () {
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        Vec::default()
    }
}

impl<'a, A0> AtomArgs<(Val,)> for (A0,)
where
    A0: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col()]
    }
}

impl<'a, A0, V0> AtomArgs<(&'a TypedVar<V0>,)> for (A0,)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    Type: FromType<V0>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col()]
    }
}

impl<'a, A0, A1> AtomArgs<(Val, Val)> for (A0, A1)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1, V1> AtomArgs<(Val, &'a TypedVar<V1>)> for (A0, A1)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1, V0> AtomArgs<(&'a TypedVar<V0>, Val)> for (A0, A1)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    Type: FromType<V0>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1, V0, V1> AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>)> for (A0, A1)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Val, Val, Val)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V2> AtomArgs<(Val, Val, &'a TypedVar<V2>)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V1> AtomArgs<(Val, &'a TypedVar<V1>, Val)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V1, V2> AtomArgs<(Val, &'a TypedVar<V1>, &'a TypedVar<V2>)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V0> AtomArgs<(&'a TypedVar<V0>, Val, Val)> for (A0, A1, A2)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    Type: FromType<V0>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V0, V2> AtomArgs<(&'a TypedVar<V0>, Val, &'a TypedVar<V2>)> for (A0, A1, A2)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    Type: FromType<V0>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V0, V1> AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>, Val)> for (A0, A1, A2)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, V0, V1, V2> AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>, &'a TypedVar<V2>)>
    for (A0, A1, A2)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(Val, Val, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V2> AtomArgs<(Val, Val, &'a TypedVar<V2>, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<Val>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V1> AtomArgs<(Val, &'a TypedVar<V1>, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    A3: AtomArg<Val>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V1, V2> AtomArgs<(Val, &'a TypedVar<V1>, &'a TypedVar<V2>, Val)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<Val>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V0> AtomArgs<(&'a TypedVar<V0>, Val, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<Val>,
    Type: FromType<V0>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V0, V2> AtomArgs<(&'a TypedVar<V0>, Val, &'a TypedVar<V2>, Val)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<Val>,
    Type: FromType<V0>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V0, V1> AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>, Val, Val)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    A3: AtomArg<Val>,
    Type: FromType<V0>,
    Type: FromType<V1>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V0, V1, V2>
    AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>, &'a TypedVar<V2>, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<Val>,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V3> AtomArgs<(Val, Val, Val, &'a TypedVar<V3>)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V2, V3> AtomArgs<(Val, Val, &'a TypedVar<V2>, &'a TypedVar<V3>)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V1, V3> AtomArgs<(Val, &'a TypedVar<V1>, Val, &'a TypedVar<V3>)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V1>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V1, V2, V3>
    AtomArgs<(Val, &'a TypedVar<V1>, &'a TypedVar<V2>, &'a TypedVar<V3>)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V0, V3> AtomArgs<(&'a TypedVar<V0>, Val, Val, &'a TypedVar<V3>)>
    for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V0>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

impl<'a, A0, A1, A2, A3, V0, V2, V3>
    AtomArgs<(&'a TypedVar<V0>, Val, &'a TypedVar<V2>, &'a TypedVar<V3>)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V0>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V0, V1, V3>
    AtomArgs<(&'a TypedVar<V0>, &'a TypedVar<V1>, Val, &'a TypedVar<V3>)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}
impl<'a, A0, A1, A2, A3, V0, V1, V2, V3>
    AtomArgs<(
        &'a TypedVar<V0>,
        &'a TypedVar<V1>,
        &'a TypedVar<V2>,
        &'a TypedVar<V3>,
    )> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a TypedVar<V0>>,
    A1: AtomArg<&'a TypedVar<V1>>,
    A2: AtomArg<&'a TypedVar<V2>>,
    A3: AtomArg<&'a TypedVar<V3>>,
    Type: FromType<V0>,
    Type: FromType<V1>,
    Type: FromType<V2>,
    Type: FromType<V3>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![
            self.0.into_col(),
            self.1.into_col(),
            self.2.into_col(),
            self.3.into_col(),
        ]
    }
}

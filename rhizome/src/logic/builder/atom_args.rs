use crate::{col_val::ColVal, id::ColId, value::Val, var::Var};

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

impl<'a, T> AtomArg<&'a Var> for (&'a str, T)
where
    T: TransitiveInto<&'a Var, ColVal>,
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

impl<'a, A0> AtomArgs<(&'a Var,)> for (A0,)
where
    A0: AtomArg<&'a Var>,
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

impl<'a, A0, A1> AtomArgs<(Val, &'a Var)> for (A0, A1)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1> AtomArgs<(&'a Var, Val)> for (A0, A1)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col()]
    }
}

impl<'a, A0, A1> AtomArgs<(&'a Var, &'a Var)> for (A0, A1)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2> AtomArgs<(Val, Val, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Val, &'a Var, Val)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Val, &'a Var, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, Val, Val)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, Val, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, &'a Var, Val)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Val>,
{
    fn into_cols(self) -> Vec<(ColId, ColVal)> {
        vec![self.0.into_col(), self.1.into_col(), self.2.into_col()]
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, &'a Var, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(Val, Val, &'a Var, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(Val, &'a Var, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(Val, &'a Var, &'a Var, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Val, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Val, &'a Var, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, Val, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, &'a Var, Val)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(Val, Val, Val, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(Val, Val, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(Val, &'a Var, Val, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(Val, &'a Var, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Val>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Val, Val, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a Var>,
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

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Val, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Val>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, Val, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Val>,
    A3: AtomArg<&'a Var>,
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
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
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

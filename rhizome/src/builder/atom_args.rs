use std::collections::HashMap;

use crate::{
    id::ColumnId,
    logic::ast::{ColumnValue, Var},
    value::Value,
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
    fn into_column(self) -> (ColumnId, ColumnValue);
}

impl<'a, T> AtomArg<Value> for (&'a str, T)
where
    T: TransitiveInto<Value, ColumnValue>,
{
    fn into_column(self) -> (ColumnId, ColumnValue) {
        (ColumnId::new(self.0), self.1.into_transitive())
    }
}

impl<'a, T> AtomArg<&'a Var> for (&'a str, T)
where
    T: TransitiveInto<&'a Var, ColumnValue>,
{
    fn into_column(self) -> (ColumnId, ColumnValue) {
        (ColumnId::new(self.0), self.1.into_transitive())
    }
}

pub trait AtomArgs<T> {
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue>;
}

impl AtomArgs<()> for () {
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        HashMap::default()
    }
}

impl<'a, A0> AtomArgs<(Value,)> for (A0,)
where
    A0: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();

        columns.insert(a0, v0);

        columns
    }
}

impl<'a, A0> AtomArgs<(&'a Var,)> for (A0,)
where
    A0: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();

        columns.insert(a0, v0);

        columns
    }
}

impl<'a, A0, A1> AtomArgs<(Value, Value)> for (A0, A1)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);

        columns
    }
}

impl<'a, A0, A1> AtomArgs<(Value, &'a Var)> for (A0, A1)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);

        columns
    }
}

impl<'a, A0, A1> AtomArgs<(&'a Var, Value)> for (A0, A1)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);

        columns
    }
}

impl<'a, A0, A1> AtomArgs<(&'a Var, &'a Var)> for (A0, A1)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Value, Value, Value)> for (A0, A1, A2)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Value, Value, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Value, &'a Var, Value)> for (A0, A1, A2)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(Value, &'a Var, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, Value, Value)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, Value, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, &'a Var, Value)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2> AtomArgs<(&'a Var, &'a Var, &'a Var)> for (A0, A1, A2)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(Value, Value, Value, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(Value, Value, &'a Var, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(Value, &'a Var, Value, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(Value, &'a Var, &'a Var, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Value, Value, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Value, &'a Var, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, Value, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, &'a Var, Value)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<Value>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(Value, Value, Value, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(Value, Value, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(Value, &'a Var, Value, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(Value, &'a Var, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<Value>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Value, Value, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<Value>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, Value, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<Value>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, Value, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<Value>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}
impl<'a, A0, A1, A2, A3> AtomArgs<(&'a Var, &'a Var, &'a Var, &'a Var)> for (A0, A1, A2, A3)
where
    A0: AtomArg<&'a Var>,
    A1: AtomArg<&'a Var>,
    A2: AtomArg<&'a Var>,
    A3: AtomArg<&'a Var>,
{
    fn into_columns(self) -> HashMap<ColumnId, ColumnValue> {
        let mut columns = HashMap::default();

        let (a0, v0) = self.0.into_column();
        let (a1, v1) = self.1.into_column();
        let (a2, v2) = self.2.into_column();
        let (a3, v3) = self.3.into_column();

        columns.insert(a0, v0);
        columns.insert(a1, v1);
        columns.insert(a2, v2);
        columns.insert(a3, v3);

        columns
    }
}

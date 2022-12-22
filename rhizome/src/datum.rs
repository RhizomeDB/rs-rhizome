use derive_more::{From, TryInto};
use ustr::Ustr;

#[derive(Debug, Clone, Copy, From, Eq, Hash, PartialEq, TryInto)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    String(Ustr),
}

impl Datum {
    pub fn bool(data: bool) -> Self {
        Self::Bool(data)
    }

    pub fn int(data: i64) -> Self {
        Self::Int(data)
    }

    pub fn string<T: AsRef<str>>(data: T) -> Self {
        let symbol = Ustr::from(data.as_ref());

        Self::String(symbol)
    }
}

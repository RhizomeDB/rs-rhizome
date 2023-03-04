use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColumnId, VarId},
    pretty::Pretty,
    value::Value,
};

use super::RelationBinding;

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Attribute(ColumnId, RelationBinding),
    Literal(Value),
    Variable(VarId),
}

impl Pretty for Term {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Term::Attribute(column_id, relation_binding) => RcDoc::concat([
                relation_binding.to_doc(),
                RcDoc::text("."),
                RcDoc::as_string(column_id),
            ]),
            Term::Literal(value) => RcDoc::as_string(value),
            Term::Variable(variable_id) => RcDoc::as_string(variable_id),
        }
    }
}

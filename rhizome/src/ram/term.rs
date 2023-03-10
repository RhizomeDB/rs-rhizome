use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColId, VarId},
    pretty::Pretty,
    value::Val,
};

use super::RelationBinding;

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Col(ColId, RelationBinding),
    Lit(Val),
    Var(VarId),
}

impl Pretty for Term {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Term::Col(col_id, relation_binding) => RcDoc::concat([
                relation_binding.to_doc(),
                RcDoc::text("."),
                RcDoc::as_string(col_id),
            ]),
            Term::Lit(value) => RcDoc::as_string(value),
            Term::Var(var_id) => RcDoc::as_string(var_id),
        }
    }
}

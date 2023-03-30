use std::sync::Arc;

use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColId, LinkId},
    pretty::Pretty,
    value::Val,
    var::Var,
};

use super::RelationBinding;

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Link(LinkId, Box<Term>),
    Col(ColId, RelationBinding),
    Lit(Arc<Val>),
    Agg(Var, RelationBinding),
}

impl Pretty for Term {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Term::Link(link_id, term) => {
                RcDoc::concat([RcDoc::as_string(link_id), RcDoc::text("@"), term.to_doc()])
            }
            Term::Col(col_id, relation_binding) => RcDoc::concat([
                relation_binding.to_doc(),
                RcDoc::text("."),
                RcDoc::as_string(col_id),
            ]),
            Term::Agg(var, relation_binding) => RcDoc::concat([
                RcDoc::text("("),
                relation_binding.to_doc(),
                RcDoc::text("."),
                RcDoc::as_string(var),
                RcDoc::text(")"),
            ]),
            Term::Lit(value) => RcDoc::as_string(value),
        }
    }
}

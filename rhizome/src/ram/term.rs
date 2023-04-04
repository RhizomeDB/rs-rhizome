use std::sync::Arc;

use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColId, LinkId, RelationId},
    pretty::Pretty,
    value::Val,
    var::Var,
};

use super::AliasId;

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Lit(Arc<Val>),
    Link(LinkId, Box<Term>),
    Col(RelationId, Option<AliasId>, ColId),
    Agg(RelationId, Option<AliasId>, Var),
}

impl Pretty for Term {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        fn relation_binding<'a>(
            relation_id: &'a RelationId,
            alias_id: &Option<AliasId>,
        ) -> RcDoc<'a, ()> {
            match alias_id {
                Some(id) => RcDoc::concat([
                    RcDoc::as_string(relation_id),
                    RcDoc::text("_"),
                    RcDoc::as_string(id),
                ]),
                None => RcDoc::as_string(relation_id),
            }
        }

        match self {
            Term::Link(link_id, term) => {
                RcDoc::concat([RcDoc::as_string(link_id), RcDoc::text("@"), term.to_doc()])
            }
            Term::Col(relation_id, alias_id, col_id) => RcDoc::concat([
                relation_binding(relation_id, alias_id),
                RcDoc::text("."),
                RcDoc::as_string(col_id),
            ]),
            Term::Agg(relation_id, alias_id, var) => RcDoc::concat([
                RcDoc::text("("),
                relation_binding(relation_id, alias_id),
                RcDoc::text("."),
                RcDoc::as_string(var),
                RcDoc::text(")"),
            ]),
            Term::Lit(value) => RcDoc::as_string(value),
        }
    }
}

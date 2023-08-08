use derive_more::{From, IsVariant, TryInto};
use pretty::RcDoc;

use crate::{
    id::{ColId, RelationId},
    pretty::Pretty,
    value::Val,
    var::Var,
};

use super::AliasId;

#[derive(Clone, Debug, From, IsVariant, TryInto)]
pub enum Term {
    Lit(Val),
    Col(RelationId, Option<AliasId>, ColId),
    Cid(RelationId, Option<AliasId>),
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
            Term::Col(relation_id, alias_id, col_id) => RcDoc::concat([
                relation_binding(relation_id, alias_id),
                RcDoc::text("."),
                RcDoc::as_string(col_id),
            ]),
            Term::Cid(relation_id, alias_id) => RcDoc::concat([
                RcDoc::text("cid("),
                relation_binding(relation_id, alias_id),
                RcDoc::text(")"),
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

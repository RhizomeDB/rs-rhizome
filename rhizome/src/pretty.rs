use pretty::RcDoc;

pub trait Pretty {
    fn to_doc(&self) -> RcDoc<'_, ()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use im::hashmap;
    use pretty_assertions::assert_eq;

    use crate::{
        id::{ColId, RelationId},
        ram::{
            alias_id::AliasId,
            formula::Formula,
            operation::{project::Project, search::Search, Operation},
            relation_binding::RelationBinding,
            relation_ref::RelationRef,
            relation_version::RelationVersion,
            term::Term,
        },
        relation::Source,
        value::Val,
    };

    use super::*;

    #[test]
    fn test() {
        let formula1 = Formula::equality(
            Term::Col(
                ColId::new("name"),
                RelationBinding::new(
                    RelationId::new("person"),
                    Some(AliasId::new().next()),
                    Source::Edb,
                ),
            ),
            Term::Lit(Arc::new(Val::String("Quinn".into()))),
        );

        let formula2 = Formula::not_in(
            [("age", Term::Lit(Arc::new(Val::U32(29))))],
            RelationRef::new(
                RelationId::new("person"),
                RelationVersion::Total,
                Source::Edb,
            ),
        );

        let project = Operation::Project(Project::new(
            hashmap! {"age" => Term::Lit(Arc::new(Val::S32(29)))},
            RelationRef::new(
                RelationId::new("person"),
                RelationVersion::Total,
                Source::Edb,
            ),
        ));

        let ast = Operation::Search(Search::new(
            RelationRef::new(
                RelationId::new("person"),
                RelationVersion::Total,
                Source::Edb,
            ),
            None,
            [formula1, formula2],
            project,
        ));

        let mut w = Vec::new();
        ast.to_doc().render(80, &mut w).unwrap();

        assert_eq!(
            r#"search person_total where
(person_1.name = "Quinn" and (age: 29) notin person_total) do
  project (age: 29) into person_total"#,
            String::from_utf8(w).unwrap()
        );
    }
}

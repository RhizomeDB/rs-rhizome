use pretty::RcDoc;

pub trait Pretty {
    fn to_doc(&self) -> RcDoc<'_, ()>;
}

#[cfg(test)]
mod tests {
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
        value::Val,
    };

    use super::*;

    #[test]
    fn test() {
        let formula1 = Formula::equality(
            Term::Col(
                ColId::new("name"),
                RelationBinding::edb(RelationId::new("person"), Some(AliasId::new().next())),
            ),
            Term::Lit(Val::String("Quinn".into())),
        );

        let formula2 = Formula::not_in(
            [("age", Term::Lit(Val::U32(29)))],
            RelationRef::edb(RelationId::new("person"), RelationVersion::Total),
        );

        let project = Operation::Project(Project::new(
            hashmap! {"age" => Term::Lit(Val::S32(29))},
            RelationRef::edb(RelationId::new("person"), RelationVersion::Total),
        ));

        let ast = Operation::Search(Search::new(
            RelationRef::edb(RelationId::new("person"), RelationVersion::Total),
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

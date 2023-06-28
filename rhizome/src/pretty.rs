use pretty::RcDoc;

pub trait Pretty {
    fn to_doc(&self) -> RcDoc<'_, ()>;
}

impl Pretty for &str {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::as_string(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use anyhow::Result;
    use im::hashmap;
    use pretty_assertions::assert_eq;

    use crate::{
        ram::{
            formula::Formula,
            operation::{project::Project, search::Search, Operation},
            term::Term,
        },
        relation::{DefaultRelation, Version},
        value::Val,
    };

    use super::*;

    #[test]
    fn test_pretty() -> Result<()> {
        let formula = Formula::not_in(
            "person".into(),
            Version::Total,
            [("age", Term::Lit(Val::U32(29)))],
            Arc::new(RwLock::new(Box::new(DefaultRelation::default()))),
        );

        let project = Operation::Project(Project::new(
            ("person".into(), Version::Total),
            hashmap! {"age" => Term::Lit(Val::S32(29))},
            vec![],
            Arc::new(RwLock::new(Box::new(DefaultRelation::default()))),
        ));

        let ast = Operation::Search(Search::new(
            ("person".into(), Version::Total),
            None,
            Arc::new(RwLock::new(Box::new(DefaultRelation::default()))),
            vec![("name".into(), Term::Lit(Val::String("Quinn".into())))],
            [formula],
            project,
        ));

        let mut w = Vec::new();
        ast.to_doc().render(80, &mut w)?;

        assert_eq!(
            r#"search person_total where
(name = "Quinn" and (age: 29) notin person_total) do
  project (age: 29) into person_total"#,
            String::from_utf8(w)?
        );

        Ok(())
    }
}

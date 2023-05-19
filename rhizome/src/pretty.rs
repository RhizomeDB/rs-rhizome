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
        fact::{DefaultEDBFact, DefaultIDBFact},
        ram::{
            formula::Formula,
            operation::{project::Project, search::Search, Operation},
            relation_version::RelationVersion,
            term::Term,
            SearchRelation,
        },
        relation::{DefaultEDBRelation, DefaultIDBRelation},
        value::Val,
    };

    use super::*;

    #[test]
    fn test_pretty() -> Result<()> {
        let formula = Formula::not_in(
            "person".into(),
            RelationVersion::Total,
            [("age", Term::Lit(Arc::new(Val::U32(29))))],
            crate::ram::NotInRelation::Edb(Arc::new(RwLock::new(DefaultEDBRelation::default()))),
        );

        let project = Operation::Project(Project::<
            DefaultEDBFact,
            DefaultIDBFact,
            DefaultEDBRelation<DefaultEDBFact>,
            DefaultIDBRelation<DefaultIDBFact>,
        >::new(
            "person".into(),
            RelationVersion::Total,
            hashmap! {"age" => Term::Lit(Arc::new(Val::S32(29)))},
            vec![],
            Arc::default(),
        ));

        let ast = Operation::Search(Search::new(
            "person".into(),
            None,
            RelationVersion::Total,
            SearchRelation::Edb(Arc::new(RwLock::new(DefaultEDBRelation::default()))),
            vec![(
                "name".into(),
                Term::Lit(Arc::new(Val::String("Quinn".into()))),
            )],
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

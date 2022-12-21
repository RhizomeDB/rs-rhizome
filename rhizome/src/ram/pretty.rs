use pretty::RcDoc;

use crate::{datum::Datum, pretty::Pretty};

use super::{
    Attribute, Equality, Formula, Literal, NotIn, Operation, Program, Relation, RelationVersion,
    Statement, Term,
};

impl Pretty for Program {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::intersperse(
            self.statements.iter().map(|statement| statement.to_doc()),
            RcDoc::text(";")
                .append(RcDoc::hardline())
                .append(RcDoc::hardline()),
        )
        .append(RcDoc::text(";"))
    }
}

impl Pretty for Relation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::as_string(self.id.clone()),
            RcDoc::text("_"),
            self.version.to_doc(),
        ])
    }
}

impl Pretty for RelationVersion {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            RelationVersion::Total => RcDoc::text("total"),
            RelationVersion::Delta => RcDoc::text("delta"),
            RelationVersion::New => RcDoc::text("new"),
        }
    }
}

impl Pretty for Statement {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Statement::Insert { operation } => RcDoc::text("insert")
                .append(RcDoc::hardline().append(operation.to_doc()).nest(2).group()),

            Statement::Merge { from, into } => RcDoc::text("merge ")
                .append(from.to_doc())
                .append(RcDoc::text(" into "))
                .append(into.to_doc()),

            Statement::Swap { left, right } => RcDoc::text("swap ")
                .append(left.to_doc())
                .append(RcDoc::text(" and "))
                .append(right.to_doc()),

            Statement::Purge { relation } => RcDoc::text("purge ").append(relation.to_doc()),

            Statement::Loop { body } => {
                let body_doc = RcDoc::hardline()
                    .append(RcDoc::intersperse(
                        body.iter().map(|statement| statement.to_doc()),
                        RcDoc::text(";")
                            .append(RcDoc::hardline())
                            .append(RcDoc::hardline()),
                    ))
                    .nest(2)
                    .group();

                RcDoc::text("loop do")
                    .append(body_doc)
                    .append(RcDoc::text(";"))
                    .append(RcDoc::hardline())
                    .append(RcDoc::text("end"))
            }

            Statement::Exit { relations } => {
                let relations_doc = RcDoc::intersperse(
                    relations.iter().map(|r| {
                        RcDoc::text("count(")
                            .append(r.to_doc())
                            .append(RcDoc::text(") == 0"))
                    }),
                    RcDoc::text(" or "),
                )
                .nest(1)
                .group();

                RcDoc::text("exit if ").append(relations_doc)
            }
        }
    }
}

impl Pretty for Operation {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Operation::Search {
                relation,
                alias,
                when,
                operation,
            } => {
                let relation_doc = match alias {
                    Some(alias) => RcDoc::concat([
                        relation.to_doc(),
                        RcDoc::text(" as "),
                        RcDoc::as_string(relation.id()),
                        RcDoc::text("_"),
                        RcDoc::as_string(alias),
                    ]),
                    None => relation.to_doc(),
                };

                let when_doc = if when.is_empty() {
                    RcDoc::nil()
                } else {
                    RcDoc::text(" where")
                        .append(RcDoc::hardline())
                        .append(RcDoc::text("("))
                        .append(
                            RcDoc::intersperse(
                                when.iter().map(|formula| formula.to_doc()),
                                RcDoc::text(" and "),
                            )
                            .nest(1)
                            .group(),
                        )
                        .append(RcDoc::text(")"))
                };

                RcDoc::concat([
                    RcDoc::text("search "),
                    relation_doc,
                    when_doc,
                    RcDoc::text(" do"),
                ])
                .append(RcDoc::hardline().append(operation.to_doc()).nest(2).group())
            }

            Operation::Project { attributes, into } => {
                let attributes_doc = RcDoc::intersperse(
                    attributes.iter().map(|(attribute, term)| {
                        RcDoc::concat([
                            RcDoc::as_string(attribute),
                            RcDoc::text(": "),
                            term.to_doc(),
                        ])
                    }),
                    RcDoc::text(",").append(RcDoc::line()),
                )
                .nest(2)
                .group();

                RcDoc::concat([
                    RcDoc::text("project "),
                    RcDoc::text("("),
                    attributes_doc,
                    RcDoc::text(")"),
                    RcDoc::text(" into "),
                    into.to_doc(),
                ])
            }
        }
    }
}

impl Pretty for Formula {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Formula::Equality(inner) => inner.to_doc(),
            Formula::NotIn(inner) => inner.to_doc(),
        }
    }
}

impl Pretty for Equality {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([self.left.to_doc(), RcDoc::text(" = "), self.right.to_doc()]).group()
    }
}

impl Pretty for NotIn {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let attributes_doc = RcDoc::intersperse(
            self.attributes.iter().map(|(attribute, term)| {
                RcDoc::concat([
                    RcDoc::as_string(attribute),
                    RcDoc::text(": "),
                    term.to_doc(),
                ])
            }),
            RcDoc::text(",").append(RcDoc::line()),
        )
        .nest(1)
        .group();

        RcDoc::concat([
            RcDoc::text("("),
            attributes_doc,
            RcDoc::text(")"),
            RcDoc::text(" notin "),
            self.relation.to_doc(),
        ])
    }
}

impl Pretty for Term {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self {
            Term::Attribute(inner) => inner.to_doc(),
            Term::Literal(inner) => inner.to_doc(),
        }
    }
}

impl Pretty for Attribute {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        let relation_doc = match self.alias() {
            Some(alias) => RcDoc::concat([
                RcDoc::as_string(self.relation.clone()),
                RcDoc::text("_"),
                RcDoc::as_string(alias),
            ]),
            None => RcDoc::as_string(self.relation.clone()),
        };

        RcDoc::concat([relation_doc, RcDoc::text("."), RcDoc::as_string(self.id())])
    }
}

impl Pretty for Literal {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        match self.datum() {
            Datum::Bool(data) => RcDoc::as_string(data),
            Datum::Int(data) => RcDoc::as_string(data),
            Datum::String(data) => RcDoc::as_string(format!("{data:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;

    use crate::ram::{Equality, NotIn};

    use super::*;

    #[test]
    fn test() {
        let formula1 = Formula::Equality(Equality {
            left: Attribute::new("name".into(), "person".into(), Some(1.into())).into(),
            right: Literal::new("Quinn".to_string()).into(),
        });

        let formula2 = NotIn::new(
            vec![
                ("age".into(), Literal::new(29).into()),
                ("foo".into(), Literal::new("bar".to_string()).into()),
            ],
            Relation::new("person".into(), RelationVersion::Total),
        )
        .into();

        let project = Operation::Project {
            attributes: BTreeMap::from_iter([
                ("age".into(), Literal::new(29).into()),
                ("foo".into(), Literal::new("bar".to_string()).into()),
            ]),
            into: Relation::new("person".into(), RelationVersion::Total),
        };

        let ast = Operation::Search {
            relation: Relation::new("person".into(), RelationVersion::Total),
            alias: None,
            when: vec![formula1, formula2],
            operation: Box::new(project),
        };

        let mut w = Vec::new();
        ast.to_doc().render(80, &mut w).unwrap();

        assert_eq!(
            r#"search person_total where
(person_1.name = "Quinn" and (age: 29, foo: "bar") notin person_total) do
  project (age: 29, foo: "bar") into person_total"#,
            String::from_utf8(w).unwrap()
        );
    }
}

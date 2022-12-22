use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_while, take_while_m_n},
    character::complete::{alphanumeric1, char, digit1, multispace1, satisfy},
    combinator::{cut, map, map_opt, map_res, opt, recognize, value, verify},
    multi::{fold_many0, many0, many0_count, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    IResult,
};

use super::ast::{
    AttributeValue, BodyTerm, Clause, Fact, Literal, Negation, Predicate, Program, Rule, Variable,
};

use crate::{
    datum::Datum,
    error::Error,
    id::{AttributeId, RelationId, VariableId},
};

pub fn parse(i: &str) -> Result<Program, Error> {
    match program(i) {
        Ok((_, program)) => Ok(program),
        // Propagate Error::* errors (RuleNotRangeRestricted, etc)
        Err(_) => Err(Error::ProgramParseError),
    }
}

fn sp(i: &str) -> IResult<&str, &str> {
    let chars = " \t\r\n";

    take_while(move |c| chars.contains(c))(i)
}

fn boolean(i: &str) -> IResult<&str, bool> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));

    alt((parse_true, parse_false))(i)
}

fn int(i: &str) -> IResult<&str, i64> {
    map_res(recognize(pair(opt(char('-')), digit1)), |s: &str| {
        s.parse::<i64>()
    })(i)
}

fn parse_unicode(i: &str) -> IResult<&str, char> {
    let parse_hex = take_while_m_n(1, 6, |c: char| c.is_ascii_hexdigit());
    let parse_delimited_hex = preceded(char('u'), delimited(char('{'), parse_hex, char('}')));
    let parse_u32 = map_res(parse_delimited_hex, move |hex| u32::from_str_radix(hex, 16));

    map_opt(parse_u32, std::char::from_u32)(i)
}

fn parse_escaped_char(i: &str) -> IResult<&str, char> {
    preceded(
        char('\\'),
        alt((
            parse_unicode,
            value('\n', char('n')),
            value('\r', char('r')),
            value('\t', char('t')),
            value('\u{08}', char('b')),
            value('\u{0C}', char('f')),
            value('\\', char('\\')),
            value('/', char('/')),
            value('"', char('"')),
        )),
    )(i)
}

fn parse_escaped_whitespace(i: &str) -> IResult<&str, &str> {
    preceded(char('\\'), multispace1)(i)
}

fn parse_literal(i: &str) -> IResult<&str, &str> {
    let not_quote_slash = is_not("\"\\");

    verify(not_quote_slash, |s: &str| !s.is_empty())(i)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
    EscapedWS,
}

fn parse_fragment(i: &str) -> IResult<&str, StringFragment<'_>> {
    alt((
        map(parse_literal, StringFragment::Literal),
        map(parse_escaped_char, StringFragment::EscapedChar),
        value(StringFragment::EscapedWS, parse_escaped_whitespace),
    ))(i)
}

fn string(i: &str) -> IResult<&str, String> {
    let build_string = fold_many0(parse_fragment, String::new, |mut string, fragment| {
        match fragment {
            StringFragment::Literal(s) => string.push_str(s),
            StringFragment::EscapedChar(c) => string.push(c),
            StringFragment::EscapedWS => {}
        }
        string
    });

    delimited(char('"'), build_string, char('"'))(i)
}

fn datum(i: &str) -> IResult<&str, Datum> {
    preceded(
        sp,
        alt((
            map(boolean, Datum::bool),
            map(int, Datum::int),
            map(string, Datum::string),
        )),
    )(i)
}

fn literal(i: &str) -> IResult<&str, Literal> {
    map(datum, Literal::new)(i)
}

fn lower_identifier(i: &str) -> IResult<&str, &str> {
    recognize(pair(
        satisfy(char::is_lowercase),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))(i)
}

fn upper_identifier(i: &str) -> IResult<&str, &str> {
    recognize(pair(
        satisfy(char::is_uppercase),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))(i)
}

fn attribute_id(i: &str) -> IResult<&str, AttributeId> {
    map(lower_identifier, AttributeId::new)(i)
}

fn relation_id(i: &str) -> IResult<&str, RelationId> {
    map(lower_identifier, RelationId::new)(i)
}

fn variable_id(i: &str) -> IResult<&str, VariableId> {
    map(upper_identifier, VariableId::new)(i)
}

fn variable(i: &str) -> IResult<&str, Variable> {
    map(variable_id, Variable::new)(i)
}

fn attribute_value(i: &str) -> IResult<&str, AttributeValue> {
    preceded(
        sp,
        alt((
            map(literal, AttributeValue::Literal),
            map(variable, AttributeValue::Variable),
        )),
    )(i)
}

fn attribute_id_value(i: &str) -> IResult<&str, (AttributeId, AttributeValue)> {
    separated_pair(
        preceded(sp, attribute_id),
        preceded(sp, char(':')),
        attribute_value,
    )(i)
}

fn attribute_id_literal(i: &str) -> IResult<&str, (AttributeId, Literal)> {
    separated_pair(preceded(sp, attribute_id), preceded(sp, char(':')), literal)(i)
}

fn arguments(i: &str) -> IResult<&str, Vec<(AttributeId, AttributeValue)>> {
    preceded(
        char('('),
        terminated(
            separated_list1(preceded(sp, char(',')), attribute_id_value),
            preceded(sp, char(')')),
        ),
    )(i)
}

fn attributes(i: &str) -> IResult<&str, Vec<(AttributeId, Literal)>> {
    preceded(
        char('('),
        terminated(
            separated_list1(preceded(sp, char(',')), attribute_id_literal),
            preceded(sp, char(')')),
        ),
    )(i)
}

fn predicate(i: &str) -> IResult<&str, Predicate> {
    map(pair(relation_id, arguments), |(relation_id, arguments)| {
        Predicate::new(relation_id, arguments)
    })(i)
}

fn negation(i: &str) -> IResult<&str, Negation> {
    preceded(
        char('!'),
        map(pair(relation_id, arguments), |(relation_id, arguments)| {
            Negation::new(relation_id, arguments)
        }),
    )(i)
}

fn body_term(i: &str) -> IResult<&str, BodyTerm> {
    preceded(
        sp,
        alt((
            map(predicate, BodyTerm::from),
            map(negation, BodyTerm::from),
        )),
    )(i)
}

fn body(i: &str) -> IResult<&str, Vec<BodyTerm>> {
    preceded(
        pair(sp, tag(":-")),
        cut(terminated(
            separated_list0(preceded(sp, char(',')), body_term),
            preceded(sp, char('.')),
        )),
    )(i)
}

fn fact(i: &str) -> IResult<&str, Fact> {
    map(
        terminated(pair(relation_id, attributes), preceded(sp, char('.'))),
        |(relation_id, attributes)| Fact::new(relation_id, attributes),
    )(i)
}

fn rule(i: &str) -> IResult<&str, Rule> {
    map_res(
        tuple((relation_id, arguments, body)),
        |(relation_id, arguments, body)| Rule::new(relation_id, arguments, body),
    )(i)
}

fn clause(i: &str) -> IResult<&str, Clause> {
    preceded(sp, alt((map(fact, Clause::from), map(rule, Clause::from))))(i)
}

fn program(i: &str) -> IResult<&str, Program> {
    map(terminated(many0(preceded(sp, clause)), sp), Program::new)(i)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_boolean() {
        assert_eq!(boolean("true"), Ok(("", true)));
        assert_eq!(boolean("false"), Ok(("", false)));
    }

    #[test]
    fn test_int() {
        assert_eq!(int("0"), Ok(("", 0)));
        assert_eq!(int("-1"), Ok(("", -1)));
        assert_eq!(int("1"), Ok(("", 1)));
        assert_eq!(int("2"), Ok(("", 2)));
        assert_eq!(int("3"), Ok(("", 3)));
        assert_eq!(int("4"), Ok(("", 4)));
        assert_eq!(int("5"), Ok(("", 5)));
        assert_eq!(int("6"), Ok(("", 6)));
        assert_eq!(int("7"), Ok(("", 7)));
        assert_eq!(int("8"), Ok(("", 8)));
        assert_eq!(int("9"), Ok(("", 9)));
        assert_eq!(int("-123456789"), Ok(("", -123456789)));
    }

    #[test]
    fn test_string() {
        assert_eq!(string(r#""hello""#), Ok(("", "hello".to_string())));
        assert_eq!(
            string(r#""hello \"world\"""#),
            Ok(("", "hello \"world\"".to_string()))
        );
    }

    #[test]
    fn test_datum() {
        assert_eq!(datum("true"), Ok(("", Datum::bool(true))));
        assert_eq!(datum("-158"), Ok(("", Datum::int(-158))));
        assert_eq!(
            datum(r#""Emoji! \"👀\"""#),
            Ok(("", Datum::string("Emoji! \"👀\"")))
        );
    }

    #[test]
    fn test_literal() {
        assert_eq!(literal("true"), Ok(("", Literal::new(true))));
        assert_eq!(literal("-158"), Ok(("", Literal::new(-158))));
        assert_eq!(
            literal(r#""Emoji! \"👀\"""#),
            Ok(("", Literal::new(Datum::string("Emoji! \"👀\""))))
        );
    }

    #[test]
    fn test_relation_id() {
        assert_eq!(relation_id("x"), Ok(("", "x".into())));
        assert_eq!(relation_id("xY"), Ok(("", "xY".into())));
        assert_eq!(relation_id("xy"), Ok(("", "xy".into())));
        assert_eq!(relation_id("x_y"), Ok(("", "x_y".into())));
    }

    #[test]
    fn test_variable_id() {
        assert_eq!(variable_id("X"), Ok(("", "X".into())));
        assert_eq!(variable_id("XY"), Ok(("", "XY".into())));
        assert_eq!(variable_id("Xy"), Ok(("", "Xy".into())));
        assert_eq!(variable_id("X_Y"), Ok(("", "X_Y".into())));
    }

    #[test]
    fn test_variable() {
        assert_eq!(variable("X"), Ok(("", Variable::new("X"))));
        assert_eq!(variable("XY"), Ok(("", Variable::new("XY"))));
        assert_eq!(variable("Xy"), Ok(("", Variable::new("Xy"))));
    }

    #[test]
    fn test_attribute_value() {
        assert_eq!(attribute_value("X"), Ok(("", Variable::new("X").into())));
        assert_eq!(attribute_value("true"), Ok(("", Literal::new(true).into())));
    }

    #[test]
    fn test_attribute_id_value() {
        assert_eq!(
            attribute_id_value("a: X"),
            Ok(("", ("a".into(), Variable::new("X").into())))
        );
        assert_eq!(
            attribute_id_value("bA_c_D:      true"),
            Ok(("", ("bA_c_D".into(), Literal::new(true).into())))
        );
    }

    #[test]
    fn test_arguments() {
        assert_eq!(
            arguments("(a: X)"),
            Ok(("", vec![("a".into(), Variable::new("X").into())]))
        );

        assert_eq!(
            arguments("(    a: X,      b: Y   )"),
            Ok((
                "",
                vec![
                    ("a".into(), Variable::new("X").into()),
                    ("b".into(), Variable::new("Y").into())
                ]
            ))
        );
    }

    #[test]
    fn test_predicate() {
        assert_eq!(
            predicate("foo(x: 1 )"),
            Ok((
                "",
                Predicate::new("foo", vec![("x", Literal::new(1).into())])
            ))
        );

        assert_eq!(
            predicate("foo(a: X, b: 2)"),
            Ok((
                "",
                Predicate::new(
                    "foo",
                    vec![
                        ("a", Variable::new("X").into()),
                        ("b", Literal::new(2).into())
                    ]
                )
            ))
        );
    }

    #[test]
    fn test_negation() {
        assert_eq!(
            negation("!foo(x: 1)"),
            Ok((
                "",
                Negation::new("foo", vec![("x", Literal::new(1).into())])
            ))
        );

        assert_eq!(
            negation("!foo(a: X, b: 2)"),
            Ok((
                "",
                Negation::new(
                    "foo",
                    vec![
                        ("a", Variable::new("X").into()),
                        ("b", Literal::new(2).into())
                    ]
                )
            ))
        );
    }

    #[test]
    fn test_body_term() {
        assert_eq!(
            body_term("foo(x: 1)"),
            Ok((
                "",
                Predicate::new("foo", vec![("x", Literal::new(1).into())]).into()
            ))
        );

        assert_eq!(
            body_term("!foo(a: X, b: 2)"),
            Ok((
                "",
                Negation::new(
                    "foo",
                    vec![
                        ("a", Variable::new("X").into()),
                        ("b", Literal::new(2).into())
                    ]
                )
                .into()
            ))
        );
    }

    #[test]
    fn test_body() {
        assert_eq!(
            body(":- foo(x: 1)."),
            Ok((
                "",
                vec![Predicate::new("foo", vec![("x", Literal::new(1).into())]).into()]
            ))
        );

        assert_eq!(
            body("   :-    foo(x: 1),      !foo(a: X, b: 2)   ."),
            Ok((
                "",
                vec![
                    Predicate::new("foo", vec![("x", Literal::new(1).into())]).into(),
                    Negation::new(
                        "foo",
                        vec![
                            ("a", Variable::new("X").into()),
                            ("b", Literal::new(2).into())
                        ]
                    )
                    .into()
                ]
            ))
        );
    }

    #[test]
    fn test_fact() {
        assert_eq!(
            fact("foo(x: 1)."),
            Ok(("", Fact::new("foo", vec![("x".into(), Literal::new(1))])))
        );

        assert_eq!(
            fact("foo(    x: 1    ,    y   :    2)    ."),
            Ok((
                "",
                Fact::new(
                    "foo",
                    vec![("x".into(), Literal::new(1)), ("y".into(), Literal::new(2))]
                )
            ))
        );
    }

    #[test]
    fn test_rule() {
        assert_eq!(
            rule("foo(x: X) :- bar(x: X)."),
            Ok((
                "",
                Rule::new(
                    "foo",
                    vec![("x", Variable::new("X").into())],
                    vec![Predicate::new("bar", vec![("x", Variable::new("X").into())]).into()]
                )
                .unwrap()
            ))
        );

        assert_eq!(
            rule("foo(x: X)    :-    bar(x: X)    ,    !baz(x: X)   ."),
            Ok((
                "",
                Rule::new(
                    "foo",
                    vec![("x", Variable::new("X").into())],
                    vec![
                        Predicate::new("bar", vec![("x", Variable::new("X").into())]).into(),
                        Negation::new("baz", vec![("x", Variable::new("X").into())]).into()
                    ]
                )
                .unwrap()
            ))
        );
    }

    #[test]
    fn test_clause() {
        assert_eq!(
            clause("foo(x: 5)."),
            Ok((
                "",
                Fact::new("foo", vec![("x".into(), Literal::new(5))]).into()
            ))
        );

        assert_eq!(
            clause("foo(x: X)    :-    bar(x: X)    ,    !baz(x: X)   ."),
            Ok((
                "",
                Rule::new(
                    "foo",
                    vec![("x", Variable::new("X").into())],
                    vec![
                        Predicate::new("bar", vec![("x", Variable::new("X").into())]).into(),
                        Negation::new("baz", vec![("x", Variable::new("X").into())]).into()
                    ]
                )
                .unwrap()
                .into()
            ))
        );
    }

    #[test]
    fn test_program() {
        assert_eq!(
            program(
                r#"
            v(v: X) :- r(r0: X, r1: Y).
            v(v: Y) :- r(r0: X, r1: Y).

            t(t0: X, t1: Y) :- r(r0: X, r1: Y).
            t(t0: X, t1: Y) :- t(t0: X, t1: Z), r(r0: Z, r1: Y).

            tc(tc0: X, tc1: Y):- v(v: X), v(v: Y), !t(t0: X, t1: Y).
            "#
            ),
            Ok((
                "",
                Program::new(vec![
                    Rule::new(
                        "v",
                        vec![("v", Variable::new("X").into())],
                        vec![Predicate::new(
                            "r",
                            vec![
                                ("r0", Variable::new("X").into()),
                                ("r1", Variable::new("Y").into()),
                            ],
                        )
                        .into()],
                    )
                    .unwrap()
                    .into(),
                    Rule::new(
                        "v",
                        vec![("v", Variable::new("Y").into())],
                        vec![Predicate::new(
                            "r",
                            vec![
                                ("r0", Variable::new("X").into()),
                                ("r1", Variable::new("Y").into()),
                            ],
                        )
                        .into()],
                    )
                    .unwrap()
                    .into(),
                    Rule::new(
                        "t",
                        vec![
                            ("t0", Variable::new("X").into()),
                            ("t1", Variable::new("Y").into()),
                        ],
                        vec![Predicate::new(
                            "r",
                            vec![
                                ("r0", Variable::new("X").into()),
                                ("r1", Variable::new("Y").into()),
                            ],
                        )
                        .into()],
                    )
                    .unwrap()
                    .into(),
                    Rule::new(
                        "t",
                        vec![
                            ("t0", Variable::new("X").into()),
                            ("t1", Variable::new("Y").into()),
                        ],
                        vec![
                            Predicate::new(
                                "t",
                                vec![
                                    ("t0", Variable::new("X").into()),
                                    ("t1", Variable::new("Z").into()),
                                ],
                            )
                            .into(),
                            Predicate::new(
                                "r",
                                vec![
                                    ("r0", Variable::new("Z").into()),
                                    ("r1", Variable::new("Y").into()),
                                ],
                            )
                            .into(),
                        ],
                    )
                    .unwrap()
                    .into(),
                    Rule::new(
                        "tc",
                        vec![
                            ("tc0", Variable::new("X").into()),
                            ("tc1", Variable::new("Y").into()),
                        ],
                        vec![
                            Predicate::new("v", vec![("v", Variable::new("X").into())]).into(),
                            Predicate::new("v", vec![("v", Variable::new("Y").into())]).into(),
                            Negation::new(
                                "t",
                                vec![
                                    ("t0", Variable::new("X").into()),
                                    ("t1", Variable::new("Y").into()),
                                ],
                            )
                            .into(),
                        ],
                    )
                    .unwrap()
                    .into(),
                ],)
            ))
        );
    }
}

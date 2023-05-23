use anyhow::Result;

use crate::{
    fact::traits::{EDBFact, IDBFact},
    ram::Program,
    relation::Relation,
};

pub use self::{program::ProgramBuilder, rule_vars::RuleVars};

use super::lower_to_ram;

mod atom_binding;
mod atom_bindings;
mod declaration;
mod fact;
mod negation;
mod program;
mod reduce;
mod rel_predicate;
mod rule_body;
mod rule_head;
mod rule_vars;
mod typed_vars_tuple;

pub fn build<E, I, ER, IR, F>(f: F) -> Result<Program<E, I, ER, IR>>
where
    E: EDBFact,
    I: IDBFact,
    ER: Relation<Fact = E>,
    IR: Relation<Fact = I>,
    F: FnOnce(ProgramBuilder) -> Result<ProgramBuilder>,
{
    let logic = ProgramBuilder::build(f)?;
    let ram = lower_to_ram::lower_to_ram::<E, I, ER, IR>(&logic)?;

    Ok(ram)
}

#[cfg(test)]
mod tests {
    use std::cmp;

    use anyhow::Result;
    use cid::Cid;

    use crate::{
        assert_compile, assert_compile_err,
        col_val::ColVal,
        error::Error,
        types::{Any, ColType, Type},
        value::Val,
        var::{TypedVar, Var},
    };

    #[test]
    fn test_tc() {
        assert_compile!(|p| {
            p.input("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

            p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                h.bind((("from", x), ("to", y)))?;
                b.search("edge", (("from", x), ("to", y)))?;

                Ok(())
            })?;

            p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                h.bind((("from", x), ("to", z)))?;

                b.search("edge", (("from", x), ("to", y)))?;
                b.search("path", (("from", y), ("to", z)))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_bind_one() {
        assert_compile!(|p| {
            p.input("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

            p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                h.bind_one(("from", x))?;
                h.bind_one(("to", y))?;

                b.build_search("edge", None, |s| {
                    s.bind_one(("from", x))?;
                    s.bind_one(("to", y))?;

                    Ok(())
                })?;

                Ok(())
            })?;

            p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                h.bind((("from", x), ("to", z)))?;

                b.build_search("edge", None, |s| {
                    s.bind_one(("from", x))?;
                    s.bind_one(("to", y))?;

                    Ok(())
                })?;

                b.build_search("path", None, |s| {
                    s.bind_one(("from", y))?;
                    s.bind_one(("to", z))?;

                    Ok(())
                })?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_get_link() -> Result<()> {
        let cid = Cid::try_from("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")?;

        assert_compile!(|p| {
            p.input("evac", |h| h)?;
            p.output("p", |h| h.column::<Cid>("x"))?;

            p.rule::<(Cid,)>("p", &|h, b, (x,)| {
                h.bind((("x", x),))?;
                b.get_link(cid, "link", x)?;

                Ok(())
            })?;

            Ok(p)
        });

        Ok(())
    }

    #[test]
    fn test_stratifiable_negation() {
        assert_compile!(|p| {
            p.input("r", |h| h.column::<i32>("r0").column::<i32>("r1"))?;

            p.output("v", |h| h.column::<i32>("v"))?;
            p.output("t", |h| h.column::<i32>("t0").column::<i32>("t1"))?;
            p.output("tc", |h| h.column::<i32>("tc0").column::<i32>("tc1"))?;

            p.rule::<(i32, i32)>("v", &|h, b, (x, y)| {
                h.bind((("v", x),))?;
                b.search("r", (("r0", x), ("r1", y)))?;

                Ok(())
            })?;

            p.rule::<(i32, i32)>("v", &|h, b, (x, y)| {
                h.bind((("v", y),))?;
                b.search("r", (("r0", x), ("r1", y)))?;

                Ok(())
            })?;

            p.rule::<(i32, i32)>("t", &|h, b, (x, y)| {
                h.bind((("t0", x), ("t1", y)))?;
                b.search("r", (("r0", x), ("r1", y)))?;

                Ok(())
            })?;

            p.rule::<(i32, i32, i32)>("t", &|h, b, (x, y, z)| {
                h.bind((("t0", x), ("t1", y)))?;

                b.search("t", (("t0", x), ("t1", z)))?;
                b.search("r", (("r0", z), ("r1", y)))?;

                Ok(())
            })?;

            p.rule::<(i32, i32)>("tc", &|h, b, (x, y)| {
                h.bind((("tc0", x), ("tc1", y)))?;

                b.search("v", (("v", x),))?;
                b.search("v", (("v", y),))?;
                b.except("t", (("t0", x), ("t1", y)))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_cyclic_negation() {
        assert_compile_err!(&Error::ProgramUnstratifiable, |p| {
            p.input("t", |h| h.column::<i32>("t"))?;

            p.output("p", |h| h.column::<i32>("p"))?;
            p.output("q", |h| h.column::<i32>("q"))?;

            p.rule::<(i32,)>("p", &|h, b, (x,)| {
                h.bind((("p", x),))?;

                b.search("t", (("t", x),))?;
                b.except("q", (("q", x),))?;

                Ok(())
            })?;

            p.rule::<(i32,)>("q", &|h, b, (x,)| {
                h.bind((("q", x),))?;

                b.search("t", (("t", x),))?;
                b.except("p", (("p", x),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_range_restriction() {
        assert_compile_err!(
            &Error::ClauseNotRangeRestricted("p0".into(), "x0".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("p0"))?;
                p.output("q", |h| h.column::<i32>("q0"))?;

                p.rule::<(i32, i32)>("p", &|h, b, (x, y)| {
                    h.bind((("p0", x),))?;
                    b.search("q", (("q0", y),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_domain_independence() {
        assert_compile_err!(&Error::ClauseNotDomainIndependent("x0".into()), |p| {
            p.output("p", |h| h.column::<i32>("p0"))?;
            p.output("q", |h| h.column::<i32>("q0"))?;

            p.rule::<(i32,)>("p", &|h, b, (x,)| {
                h.bind((("p0", x),))?;
                b.except("q", (("q0", x),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_fact_edb() {
        assert_compile_err!(&Error::ClauseHeadEDB("p".into()), |p| {
            p.input("p", |h| h.column::<i32>("p0"))?;

            p.fact("p", |f| f.bind((("p0", 1),)))?;

            Ok(p)
        });
    }

    #[test]
    fn test_rule_edb() {
        assert_compile_err!(&Error::ClauseHeadEDB("p".into()), |p| {
            p.input("p", |h| h.column::<i32>("p0"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("p0", 1),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_fact_unrecognized() {
        assert_compile_err!(&Error::UnrecognizedRelation("p".into()), |p| {
            p.fact("p", |f| f.bind((("p0", 1),)))?;

            Ok(p)
        });
    }

    #[test]
    fn test_rule_unrecognized() {
        assert_compile_err!(&Error::UnrecognizedRelation("p".into()), |p| {
            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("p0", 1),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_duplicate_input_declaration_column() {
        assert_compile_err!(
            &Error::DuplicateDeclarationCol("p".into(), "x".into()),
            |p| {
                p.input("p", |h| h.column::<i32>("x").column::<i32>("x"))?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_duplicate_output_declaration_column() {
        assert_compile_err!(
            &Error::DuplicateDeclarationCol("q".into(), "y".into()),
            |p| {
                p.output("q", |h| h.column::<i32>("y").column::<i32>("y"))?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_conflicting_declaration() {
        assert_compile_err!(&Error::ConflictingRelationDeclaration("p".into()), |p| {
            p.input("p", |h| h.column::<i32>("x"))?;
            p.input("p", |h| h.column::<i32>("x"))?;

            Ok(p)
        });

        assert_compile_err!(&Error::ConflictingRelationDeclaration("p".into()), |p| {
            p.input("p", |h| h.column::<i32>("x"))?;
            p.output("p", |h| h.column::<i32>("x"))?;

            Ok(p)
        });

        assert_compile_err!(&Error::ConflictingRelationDeclaration("p".into()), |p| {
            p.output("p", |h| h.column::<i32>("x"))?;
            p.input("p", |h| h.column::<i32>("x"))?;

            Ok(p)
        });

        assert_compile_err!(&Error::ConflictingRelationDeclaration("p".into()), |p| {
            p.output("p", |h| h.column::<i32>("x"))?;
            p.output("p", |h| h.column::<i32>("x"))?;

            Ok(p)
        });
    }

    #[test]
    fn test_conflicting_column_binding_fact() {
        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("x", 1), ("x", 1))))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("x", 1), ("x", 2))))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.fact("p", |f| f.bind((("x", 1), ("y", 2), ("x", 1))))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.fact("p", |f| f.bind((("y", 2), ("x", 1), ("x", 3))))?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_conflicting_column_binding_rule() {
        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", 1), ("x", 1)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", 1), ("x", 2)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", 1), ("y", 1), ("x", 1)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ConflictingColumnBinding("p".into(), "x".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("y", 2), ("x", 1), ("x", 3)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_unrecognized_column_binding_fact() {
        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("y", 1),)))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("x", 1), ("y", 2))))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("y", 2), ("x", 1))))?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_unrecognized_column_binding_rule() {
        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("y", 1),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", 1), ("y", 2)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::UnrecognizedColumnBinding("p".into(), "y".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("y", 2), ("x", 1)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_nonground_fact() {
        assert_compile_err!(
            &Error::NonGroundFact("p".into(), "x".into(), "foo".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("x", TypedVar::<i32>::new("foo")),)))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::NonGroundFact("p".into(), "y".into(), "foo".into()),
            |p| {
                p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.fact("p", |f| {
                    f.bind((("x", 1), ("y", TypedVar::<i32>::new("foo"))))
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_missing_fact() {
        assert_compile_err!(&Error::ColumnMissing("p".into(), "x".into()), |p| {
            p.output("p", |h| h.column::<i32>("x"))?;

            p.fact("p", |f| f)?;

            Ok(p)
        });

        assert_compile_err!(&Error::ColumnMissing("p".into(), "x".into()), |p| {
            p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

            p.fact("p", |f| f.bind((("y", 1),)))?;

            Ok(p)
        });

        assert_compile_err!(&Error::ColumnMissing("p".into(), "y".into()), |p| {
            p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

            p.fact("p", |f| f.bind((("x", 1),)))?;

            Ok(p)
        });
    }

    #[test]
    fn test_column_missing_rule() {
        assert_compile_err!(&Error::ColumnMissing("p".into(), "x".into()), |p| {
            p.output("p", |h| h.column::<i32>("x"))?;

            p.rule::<()>("p", &|_, _, ()| Ok(()))?;

            Ok(p)
        });

        assert_compile_err!(&Error::ColumnMissing("p".into(), "x".into()), |p| {
            p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("y", 1),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile_err!(&Error::ColumnMissing("p".into(), "y".into()), |p| {
            p.output("p", |h| h.column::<i32>("x").column::<i32>("y"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_var_type_conflict_get_link_cid() {
        assert_compile_err!(
            &Error::VarTypeConflict(Var::new::<i32>("x0"), Type::Cid),
            |p| {
                p.input("evac", |h| h)?;
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<(i32, Cid)>("p", &|h, b, (x, y)| {
                    h.bind((("x", x),))?;
                    b.get_link(x, "link", y)?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_var_type_conflict_get_link_value() -> Result<()> {
        let cid = Cid::try_from("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")?;

        assert_compile_err!(
            &Error::VarTypeConflict(Var::new::<i32>("x0"), Type::Cid),
            |p| {
                p.input("evac", |h| h)?;
                p.output("p", |h| h.column::<i32>("x"))?;

                p.rule::<(i32,)>("p", &|h, b, (x,)| {
                    h.bind((("x", x),))?;
                    b.get_link(cid, "link", x)?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        Ok(())
    }

    #[test]
    fn test_column_value_type_conflict_fact_literal() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit(Val::S8(5)),
                ColType::Type(Type::S32)
            ),
            |p| {
                p.output("p", |h| h.column::<i32>("x"))?;

                p.fact("p", |f| f.bind((("x", 5_i8),)))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit("foo".into()),
                ColType::Type(Type::Cid)
            ),
            |p| {
                p.output("p", |h| h.column::<Cid>("x"))?;

                p.fact("p", |f| f.bind((("x", "foo"),)))?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit(Val::Char('f')),
                ColType::Type(Type::String)
            ),
            |p| {
                p.output("p", |h| h.column::<&str>("x"))?;

                p.fact("p", |f| f.bind((("x", 'f'),)))?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_value_type_conflict_rule_head_literal() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit(Val::U16(8)),
                ColType::Type(Type::Bool)
            ),
            |p| {
                p.output("p", |h| h.column::<bool>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", 8_u16),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit(Val::Bool(true)),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.output("p", |h| h.column::<u32>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", true),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Lit("b".into()),
                ColType::Type(Type::Char)
            ),
            |p| {
                p.output("p", |h| h.column::<char>("x"))?;

                p.rule::<()>("p", &|h, _, ()| {
                    h.bind((("x", "b"),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_value_type_conflict_rule_body_literal() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "x".into(),
                ColVal::Lit(Val::U16(8)),
                ColType::Type(Type::Bool)
            ),
            |p| {
                p.input("q", |h| h.column::<bool>("x"))?;
                p.output("p", |h| h.column::<bool>("y"))?;

                p.rule::<(bool,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", 8_u16),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "x".into(),
                ColVal::Lit(Val::Bool(true)),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.input("q", |h| h.column::<u32>("x"))?;
                p.output("p", |h| h.column::<u32>("y"))?;

                p.rule::<(u32,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", true),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "x".into(),
                ColVal::Lit("b".into()),
                ColType::Type(Type::Char)
            ),
            |p| {
                p.input("q", |h| h.column::<char>("x"))?;
                p.output("p", |h| h.column::<char>("y"))?;

                p.rule::<(char,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", "b"),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_value_type_conflict_rule_head_var() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "y".into(),
                ColVal::Binding(Var::new::<u32>("x0")),
                ColType::Type(Type::Bool)
            ),
            |p| {
                p.input("q", |h| h.column::<u32>("x"))?;
                p.output("p", |h| h.column::<bool>("y"))?;

                p.rule::<(u32,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", x),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_value_type_conflict_rule_body_var() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "x".into(),
                ColVal::Binding(Var::new::<bool>("x0")),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.input("q", |h| h.column::<u32>("x"))?;
                p.output("p", |h| h.column::<bool>("y"))?;

                p.rule::<(bool,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", x),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_column_value_type_conflict_rule_downcast_var() {
        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "y".into(),
                ColVal::Binding(Var::new::<bool>("x0")),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.input("q", |h| h.column::<Any>("x").column::<u32>("y"))?;
                p.output("p", |h| h.column::<bool>("y"))?;

                p.rule::<(bool,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("x", x), ("y", x)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "q".into(),
                "y".into(),
                ColVal::Binding(Var::new::<bool>("x0")),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.input("q", |h| h.column::<Any>("x").column::<u32>("y"))?;
                p.output("p", |h| h.column::<bool>("y"))?;

                p.rule::<(bool,)>("p", &|h, b, (x,)| {
                    h.bind((("y", x),))?;
                    b.search("q", (("y", x), ("x", x)))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "x".into(),
                ColVal::Binding(Var::new::<bool>("x0")),
                ColType::Type(Type::U32)
            ),
            |p| {
                p.input("q", |h| h.column::<Any>("x"))?;
                p.output("p", |h| h.column::<u32>("x").column::<bool>("y"))?;

                p.rule::<(bool,)>("p", &|h, b, (x,)| {
                    h.bind((("x", x), ("y", x)))?;
                    b.search("q", (("x", x),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );

        assert_compile_err!(
            &Error::ColumnValueTypeConflict(
                "p".into(),
                "y".into(),
                ColVal::Binding(Var::new::<u32>("x0")),
                ColType::Type(Type::Bool)
            ),
            |p| {
                p.input("q", |h| h.column::<Any>("x"))?;
                p.output("p", |h| h.column::<u32>("x").column::<bool>("y"))?;

                p.rule::<(u32,)>("p", &|h, b, (x,)| {
                    h.bind((("x", x), ("y", x)))?;
                    b.search("q", (("x", x),))?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_fact_literal_types() -> Result<()> {
        assert_compile!(|p| {
            p.output("p", |h| h.column::<bool>("x"))?;

            p.fact("p", |f| f.bind((("x", true),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i8>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_i8),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u8>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_u8),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i16>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_i16),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u16>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_u16),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i32>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_i32),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u32>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_u32),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i64>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_i64),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u64>("x"))?;

            p.fact("p", |f| f.bind((("x", 1_u64),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<char>("x"))?;

            p.fact("p", |f| f.bind((("x", 'c'),)))?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<&str>("x"))?;

            p.fact("p", |f| f.bind((("x", "test"),)))?;

            Ok(p)
        });

        let cid = Cid::try_from("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")?;

        assert_compile!(|p| {
            p.output("p", |h| h.column::<Cid>("x"))?;

            p.fact("p", |f| f.bind((("x", cid),)))?;

            Ok(p)
        });

        Ok(())
    }

    #[test]
    fn test_rule_head_literal_types() -> Result<()> {
        assert_compile!(|p| {
            p.output("p", |h| h.column::<bool>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", true),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i8>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_i8),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u8>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_u8),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i16>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_i16),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u16>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_u16),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i32>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_i32),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u32>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_u32),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<i64>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_i64),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<u64>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 1_u64),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<char>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", 'c'),))?;

                Ok(())
            })?;

            Ok(p)
        });

        assert_compile!(|p| {
            p.output("p", |h| h.column::<&str>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", "test"),))?;

                Ok(())
            })?;

            Ok(p)
        });

        let cid = Cid::try_from("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")?;

        assert_compile!(|p| {
            p.output("p", |h| h.column::<Cid>("x"))?;

            p.rule::<()>("p", &|h, _, ()| {
                h.bind((("x", cid),))?;

                Ok(())
            })?;

            Ok(p)
        });

        Ok(())
    }

    #[test]
    fn test_downcast_into_head() {
        assert_compile!(|p| {
            p.input("q", |h| h.column::<Any>("x"))?;
            p.output("p", |h| h.column::<u32>("x"))?;

            p.rule::<(u32,)>("p", &|h, b, (x,)| {
                h.bind((("x", x),))?;
                b.search("q", (("x", x),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_upcast_into_head() {
        assert_compile!(|p| {
            p.input("q", |h| h.column::<u32>("x"))?;
            p.output("p", |h| h.column::<Any>("x"))?;

            p.rule::<(u32,)>("p", &|h, b, (x,)| {
                h.bind((("x", x),))?;
                b.search("q", (("x", x),))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_predicate() {
        assert_compile!(|p| {
            p.input("num", |h| h.column::<i32>("n"))?;
            p.output("triangle", |h| {
                h.column::<i32>("a").column::<i32>("b").column::<i32>("c")
            })?;

            p.rule::<(i32, i32, i32)>("triangle", &|h, b, (x, y, z)| {
                h.bind((("a", x), ("b", y), ("c", z)))?;

                b.search("num", (("n", x),))?;
                b.search("num", (("n", y),))?;
                b.search("num", (("n", z),))?;
                b.predicate((x, y, z), |(x, y, z)| x + y < z)?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_reduce() {
        assert_compile!(|p| {
            p.input("num", |h| h.column::<i32>("n"))?;
            p.output("sum", |h| h.column::<i32>("n"))?;

            p.rule::<(i32, i32)>("sum", &|h, b, (sum, n)| {
                h.bind((("n", sum),))?;

                b.reduce(sum, (n,), "num", (("n", n),), 0, |acc, (x,)| acc + x)?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_variadic_reduce() {
        assert_compile!(|p| {
            p.input("pair", |h| h.column::<i32>("x").column::<i32>("y"))?;
            p.output("minSum", |h| h.column::<i32>("n"))?;

            p.rule::<(i32, i32, i32)>("minSum", &|h, b, (sum, x, y)| {
                h.bind((("n", sum),))?;

                b.reduce(
                    sum,
                    (x, y),
                    "pair",
                    (("x", x), ("y", y)),
                    0,
                    |acc, (x, y)| acc + cmp::min(x, y),
                )?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_group_by_reduce() {
        assert_compile!(|p| {
            p.input("product", |h| {
                h.column::<i32>("id")
                    .column::<i32>("category")
                    .column::<i32>("stock")
            })?;

            p.output("categoryStock", |h| {
                h.column::<i32>("category").column::<i32>("stock")
            })?;

            p.rule::<(i32, i32, i32)>("categoryStock", &|h,
                                                         b,
                                                         (
                category,
                category_stock,
                product_stock,
            )| {
                h.bind((("category", category), ("stock", category_stock)))?;

                b.search("product", (("category", category),))?;
                b.reduce(
                    category_stock,
                    (product_stock,),
                    "product",
                    (("category", category), ("stock", product_stock)),
                    0,
                    |acc, (x,)| acc + x,
                )?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_reduce_unbound_group_by() {
        assert_compile_err!(
            &Error::ClauseNotRangeRestricted("x".into(), "x1".into(),),
            |p| {
                p.input("pair", |h| h.column::<i32>("x").column::<i32>("y"))?;
                p.output("ySum", |h| h.column::<i32>("x").column::<i32>("y"))?;

                p.rule::<(i32, i32, i32)>("ySum", &|h, b, (sum, x, y)| {
                    h.bind((("x", x), ("y", sum)))?;

                    b.reduce(sum, (y,), "pair", (("x", x), ("y", y)), 0, |acc, (y,)| {
                        acc + y
                    })?;

                    Ok(())
                })?;

                Ok(p)
            }
        );
    }

    #[test]
    fn test_reduce_bound_target() {
        assert_compile_err!(&Error::ReduceBoundTarget("x0".into()), |p| {
            p.input("num", |h| h.column::<i32>("n"))?;
            p.output("sum", |h| h.column::<i32>("n"))?;

            p.rule::<(i32, i32)>("sum", &|h, b, (sum, n)| {
                h.bind((("n", sum),))?;

                b.search("num", (("n", sum),))?;
                b.reduce(sum, (n,), "num", (("n", n),), 0, |acc, (x,)| acc + x)?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_reduce_group_by_target() {
        assert_compile_err!(&Error::ReduceGroupByTarget("x0".into()), |p| {
            p.input("num", |h| h.column::<i32>("n"))?;
            p.output("sum", |h| h.column::<i32>("n"))?;

            p.rule::<(i32,)>("sum", &|h, b, (n,)| {
                h.bind((("n", n),))?;

                b.reduce(n, (n,), "num", (("n", n),), 0, |acc, (x,)| acc + x)?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_array() {
        assert_compile!(|p| {
            p.input("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

            p.rule::<[i32; 2]>("path", &|h, b, [x, y]| {
                h.bind((("from", x), ("to", y)))?;
                b.search("edge", (("from", x), ("to", y)))?;

                Ok(())
            })?;

            p.rule::<[i32; 3]>("path", &|h, b, [x, y, z]| {
                h.bind((("from", x), ("to", z)))?;

                b.search("edge", (("from", x), ("to", y)))?;
                b.search("path", (("from", y), ("to", z)))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_array_reduce() {
        assert_compile!(|p| {
            p.input("num", |h| h.column::<i32>("n"))?;
            p.output("sum", |h| h.column::<i32>("n"))?;

            p.rule::<[i32; 2]>("sum", &|h, b, [sum, n]| {
                h.bind((("n", sum),))?;
                b.reduce(sum, (n,), "num", (("n", n),), 0, |acc, (x,)| acc + x)?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_array_any() {
        assert_compile!(|p| {
            p.input("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

            p.rule::<[Any; 2]>("path", &|h, b, [x, y]| {
                h.bind((("from", x), ("to", y)))?;
                b.search("edge", (("from", x), ("to", y)))?;

                Ok(())
            })?;

            p.rule::<[Any; 3]>("path", &|h, b, [x, y, z]| {
                h.bind((("from", x), ("to", z)))?;

                b.search("edge", (("from", x), ("to", y)))?;
                b.search("path", (("from", y), ("to", z)))?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_search_cid_edb() {
        assert_compile!(|p| {
            p.input("evac", |h| h)?;
            p.output("p", |h| h.column::<Cid>("x"))?;

            p.rule::<(Cid,)>("p", &|h, b, (x,)| {
                h.bind((("x", x),))?;

                b.search_cid("evac", x, ())?;

                Ok(())
            })?;

            Ok(p)
        });
    }

    #[test]
    fn test_search_cid_idb() {
        assert_compile_err!(&Error::ContentAddressedIDB("q".into()), |p| {
            p.output("q", |h| h)?;
            p.output("p", |h| h.column::<Cid>("x"))?;

            p.rule::<(Cid,)>("p", &|h, b, (x,)| {
                h.bind((("x", x),))?;

                b.search_cid("q", x, ())?;

                Ok(())
            })?;

            Ok(p)
        });
    }
}

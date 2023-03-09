pub mod program;
pub use program::ProgramBuilder;

mod atom_args;
mod declaration;
mod fact;
mod negation;
mod predicate;
mod rule;
mod rule_vars;

#[cfg(test)]
mod tests {
    use crate::{assert_compile, assert_compile_err, error::Error};

    #[test]
    fn test_tc() {
        assert_compile!(|p| {
            p.input("edge", |h| h.column::<i32>("from").column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from").column::<i32>("to"))?;

            p.rule::<(i32, i32)>("path", &|h, b, (x, y)| {
                (
                    h.bind((("from", x), ("to", y))),
                    b.search("edge", (("from", x), ("to", y))),
                )
            })?;

            p.rule::<(i32, i32, i32)>("path", &|h, b, (x, y, z)| {
                (
                    h.bind((("from", x), ("to", z))),
                    b.search("edge", (("from", x), ("to", y)))
                        .search("path", (("from", y), ("to", z))),
                )
            })
        });
    }

    #[test]
    fn test_stratifiable_negation() {
        assert_compile!(|p| {
            p.input("r", |h| h.column::<i32>("r0").column::<i32>("r1"))?;

            p.output("v", |h| h.column::<i32>("v"))?;
            p.output("t", |h| h.column::<i32>("t0").column::<i32>("t1"))?;
            p.output("tc", |h| h.column::<i32>("tc0").column::<i32>("tc1"))?;

            p.rule::<(i32, i32)>("v", &|h, b, (x, y)| {
                (h.bind((("v", x),)), b.search("r", (("r0", x), ("r1", y))))
            })?;

            p.rule::<(i32, i32)>("v", &|h, b, (x, y)| {
                (h.bind((("v", y),)), b.search("r", (("r0", x), ("r1", y))))
            })?;

            p.rule::<(i32, i32)>("t", &|h, b, (x, y)| {
                (
                    h.bind((("t0", x), ("t1", y))),
                    b.search("r", (("r0", x), ("r1", y))),
                )
            })?;

            p.rule::<(i32, i32, i32)>("t", &|h, b, (x, y, z)| {
                (
                    h.bind((("t0", x), ("t1", y))),
                    b.search("t", (("t0", x), ("t1", z)))
                        .search("r", (("r0", z), ("r1", y))),
                )
            })?;

            p.rule::<(i32, i32)>("tc", &|h, b, (x, y)| {
                (
                    h.bind((("tc0", x), ("tc1", y))),
                    b.search("v", (("v", x),))
                        .search("v", (("v", y),))
                        .except("t", (("t0", x), ("t1", y))),
                )
            })?;

            Ok(())
        });
    }

    #[test]
    fn test_cyclic_negation() {
        assert_compile_err!(&Error::ProgramUnstratifiable, |p| {
            p.input("t", |h| h.column::<i32>("t"))?;

            p.output("p", |h| h.column::<i32>("p"))?;
            p.output("q", |h| h.column::<i32>("q"))?;

            p.rule::<(i32,)>("p", &|h, b, (x,)| {
                (
                    h.bind((("p", x),)),
                    b.search("t", ((("t", x)),)).except("q", (("q", x),)),
                )
            })?;

            p.rule::<(i32,)>("q", &|h, b, (x,)| {
                (
                    h.bind((("q", x),)),
                    b.search("t", ((("t", x)),)).except("p", (("p", x),)),
                )
            })
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
                    (h.bind((("p0", x),)), b.search("q", (("q0", y),)))
                })
            }
        );
    }

    #[test]
    fn test_domain_independence() {
        assert_compile_err!(&Error::ClauseNotDomainIndependent("x0".into()), |p| {
            p.output("p", |h| h.column::<i32>("p0"))?;
            p.output("q", |h| h.column::<i32>("q0"))?;

            p.rule::<(i32,)>("p", &|h, b, (x,)| {
                (h.bind((("p0", x),)), b.except("q", (("q0", x),)))
            })
        });
    }
}

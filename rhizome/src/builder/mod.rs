pub mod program;
pub use program::ProgramBuilder;

mod declaration;
mod fact;
mod negation;
mod predicate;
mod rule;

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn test_builder() -> Result<()> {
        let _p = ProgramBuilder::build(|p| {
            p.input("edge", |h| h.column::<i32>("from")?.column::<i32>("to"))?;
            p.output("path", |h| h.column::<i32>("from")?.column::<i32>("to"))?;

            p.rule(
                "path",
                |h| h.bind("from", "x")?.bind("to", "y"),
                |b| b.search("edge", |s| s.bind("from", "x")?.bind("to", "y")),
            )?;

            p.rule(
                "path",
                |h| h.bind("from", "x")?.bind("to", "z"),
                |b| {
                    b.search("edge", |s| s.bind("from", "x")?.bind("to", "y"))?
                        .search("path", |s| s.bind("from", "y")?.bind("to", "z"))
                },
            )
        })?;

        Ok(())
    }
}

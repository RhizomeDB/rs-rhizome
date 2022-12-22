#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

use anyhow::Result;
use logic::{ast::Program, parser};

pub mod datum;
pub mod error;
pub mod fact;
pub mod id;
pub mod lattice;
pub mod logic;
pub mod pretty;
pub mod ram;
pub mod timestamp;

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

pub fn parse(i: &str) -> Result<Program> {
    parser::parse(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert!(parse(
            r#"
        edge(from: 0, to: 1).
        edge(from: 1, to: 2).
        edge(from: 2, to: 3).
        edge(from: 3, to: 4).

        path(from: X, to: Y) :- edge(from: X, to: Y).
        path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
        "#,
        )
        .is_ok());
    }
}

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

pub mod datum;
pub mod error;
pub mod fact;
pub mod graph;
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

/// Add two integers together.
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Multiplies two integers together.
pub fn mult(a: i32, b: i32) -> i32 {
    a * b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mult() {
        assert_eq!(mult(3, 2), 6);
    }
}

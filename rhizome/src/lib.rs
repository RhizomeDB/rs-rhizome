#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

pub mod col;
pub mod col_val;
pub mod error;
pub mod fact;
pub mod id;
pub mod interner;
pub mod lattice;
pub mod logic;
pub mod pretty;
pub mod ram;
pub mod relation;
pub mod runtime;
pub mod schema;
pub mod storage;
pub mod timestamp;
pub mod types;
pub mod value;
pub mod var;

pub use logic::build;

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

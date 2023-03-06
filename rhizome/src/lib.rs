#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

pub mod builder;
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
pub mod storage;
pub mod timestamp;
pub mod types;
pub mod value;

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

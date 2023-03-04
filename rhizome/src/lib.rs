#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! rhizome

use crate::pretty::Pretty;
use anyhow::Result;
use storage::{codec::Codec, memory::MemoryBlockstore};

use fact::traits::{EDBFact, IDBFact};
use logic::ast::program::Program;
use reactor::Reactor;
use relation::Relation;
use timestamp::Timestamp;

pub mod builder;
// pub mod datum;
pub mod error;
pub mod fact;
pub mod id;
pub mod interner;
pub mod lattice;
pub mod logic;
pub mod pretty;
pub mod ram;
pub mod reactor;
pub mod relation;
pub mod storage;
pub mod timestamp;
pub mod types;
pub mod value;
pub mod vm;

// pub fn parse(i: &str) -> Result<Program> {
//     parser::parse(i)
// }

pub fn pretty(program: &Program) -> Result<String> {
    let ram = logic::lower_to_ram::lower_to_ram(program)?;

    let mut buf = Vec::<u8>::new();
    ram.to_doc().render(80, &mut buf)?;

    Ok(String::from_utf8(buf)?)
}

pub fn run<T, C, EF, IF, ER, IR>(program: &Program, relation: &str) -> Result<IR>
where
    T: Timestamp,
    C: Codec,
    ER: Relation<EF>,
    IR: Relation<IF>,
    EF: EDBFact,
    IF: IDBFact,
{
    let ram = logic::lower_to_ram::lower_to_ram(program)?;
    let bs = MemoryBlockstore::default();
    let mut vm: vm::VM<T, EF, IF, ER, IR> = vm::VM::new(ram);

    vm.step_epoch(&bs)?;

    Ok(vm.relation(relation))
}

pub fn spawn(program: &Program) -> Result<Reactor> {
    let ram = logic::lower_to_ram::lower_to_ram(program)?;
    let vm: vm::VM = vm::VM::new(ram);
    let reactor = Reactor::new(vm);

    Ok(reactor)
}

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

#[cfg(test)]
mod tests {
    //     use super::*;

    //     #[test]
    //     fn test_parse() {
    //         assert!(parse(
    //             r#"
    //         edge(from: 0, to: 1).
    //         edge(from: 1, to: 2).
    //         edge(from: 2, to: 3).
    //         edge(from: 3, to: 4).

    //         path(from: X, to: Y) :- edge(from: X, to: Y).
    //         path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
    //         "#,
    //         )
    //         .is_ok());
    //     }
}

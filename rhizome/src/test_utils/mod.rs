/// Random value generator for sampling data.
mod rvg;

pub use rvg::*;

#[macro_export]
macro_rules! assert_compiles {
    ($program_closure:expr) => {
        let program = match $crate::builder::ProgramBuilder::build($program_closure) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to build program: {:?}", e);
            }
        };

        match $crate::logic::lower_to_ram::lower_to_ram(&program) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to lower program: {:?}", e);
            }
        };
    };
}

#[macro_export]
macro_rules! assert_derives {
    ($relation:expr, $program_closure:expr, $expected:expr) => {
        assert_derives!($relation, $program_closure, [], $expected);
    };
    ($relation:expr, $program_closure:expr, $edb:expr, $expected:expr) => {
        let program = match $crate::builder::ProgramBuilder::build($program_closure) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to build program: {:?}", e);
            }
        };

        let program = match $crate::logic::lower_to_ram::lower_to_ram(&program) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to lower program: {:?}", e);
            }
        };

        let bs = $crate::storage::memory::MemoryBlockstore::default();
        let mut vm = <$crate::runtime::vm::VM>::new(program);

        for fact in $edb {
            vm.push(fact).unwrap();
        }

        match vm.step_epoch(&bs) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to run program: {:?}", e);
            }
        };

        let mut facts = std::collections::BTreeSet::default();
        while let Ok(Some(fact)) = vm.pop() {
            if fact.id() == $relation.into() {
                facts.insert(fact);
            }
        }

        let expected = std::collections::BTreeSet::from_iter($expected);

        assert_eq!(facts, expected);
    };
}

/// Random value generator for sampling data.
mod rvg;

pub use rvg::*;

use crate::{
    fact::{DefaultEDBFact, DefaultIDBFact},
    ram::Program,
    relation::DefaultRelation,
    ProgramBuilder,
};
use anyhow::Result;

#[allow(dead_code)]
pub(crate) fn build_easy<F>(
    f: F,
) -> Result<
    Program<
        DefaultEDBFact,
        DefaultIDBFact,
        DefaultRelation<DefaultEDBFact>,
        DefaultRelation<DefaultIDBFact>,
    >,
>
where
    F: FnOnce(&mut ProgramBuilder) -> Result<()>,
{
    crate::build::<
        DefaultEDBFact,
        DefaultIDBFact,
        DefaultRelation<DefaultEDBFact>,
        DefaultRelation<DefaultIDBFact>,
        F,
    >(f)
}

#[macro_export]
macro_rules! assert_compile {
    ($program_closure:expr) => {
        match $crate::test_utils::build_easy($program_closure) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to build program: {:?}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! assert_compile_err {
    ($err:expr, $program_closure:expr) => {
        match $crate::test_utils::build_easy($program_closure) {
            std::result::Result::Ok(_) => {
                panic!("Expected an error, but compilation succeeded!");
            }
            std::result::Result::Err(e) => {
                pretty_assertions::assert_eq!(Some($err), e.downcast_ref());
            }
        };
    };
}

#[macro_export]
macro_rules! assert_derives {
    ($program_closure:expr, $expected:expr) => {
        assert_derives!(
            $program_closure,
            Vec::<$crate::fact::evac_fact::EVACFact>::default(),
            $expected
        );
    };
    ($program_closure:expr, $edb:expr, $expected:expr) => {
        let program = match $crate::build($program_closure) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to build program: {:?}", e);
            }
        };

        let mut b = Vec::default();
        $crate::pretty::Pretty::to_doc(&program)
            .render(80, &mut b)
            .unwrap();

        let pretty = String::from_utf8(b).unwrap();

        let mut bs = $crate::storage::memory::MemoryBlockstore::default();
        let mut vm = <$crate::runtime::vm::VM>::new(program);

        for fact in &$edb {
            $crate::storage::blockstore::Blockstore::put_serializable(
                &mut bs,
                fact,
                $crate::storage::DefaultCodec::default(),
                $crate::storage::DEFAULT_MULTIHASH,
            )
            .unwrap();

            vm.push(fact.clone()).unwrap();
        }

        match vm.step_epoch(&bs) {
            std::result::Result::Ok(v) => v,
            std::result::Result::Err(e) => {
                panic!("Failed to run program: {:?}", e);
            }
        };

        let mut facts = std::collections::BTreeMap::default();

        for (relation, _) in &$expected {
            facts.insert(
                $crate::id::RelationId::new(relation),
                std::collections::BTreeSet::default(),
            );
        }

        while let Ok(Some(fact)) = vm.pop() {
            if let Some(relation) = facts.get_mut(&fact.id()) {
                relation.insert(fact);
            }
        }

        for (relation, expected) in $expected {
            let actual = facts
                .get(&$crate::id::RelationId::new(relation))
                .unwrap()
                .clone();

            let expected = std::collections::BTreeSet::from_iter(expected.clone());

            pretty_assertions::assert_eq!(actual, expected, "program = \n{}", pretty);
        }
    };
}

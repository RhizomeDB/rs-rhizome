#![cfg(target_arch = "wasm32")]

//! Test suite for the Web and headless browsers.

use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use rhizome_wasm::{Fact, Program};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_transitive_closure() {
    let source = r#"
edge(from: 0, to: 1).
edge(from: 1, to: 2).
edge(from: 2, to: 3).
edge(from: 3, to: 4).

path(from: X, to: Y) :- edge(from: X, to: Y).
path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
    "#;

    let program = Program::parse(source).unwrap();
    let results = Program::run(&program, "path").unwrap();

    let expected = BTreeSet::from([
        Fact::new(
            "path".into(),
            vec![("from".into(), 1.into()), ("to".into(), 2.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 1.into()), ("to".into(), 3.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 3.into()), ("to".into(), 4.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 2.into()), ("to".into(), 3.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 0.into()), ("to".into(), 3.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 0.into()), ("to".into(), 4.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 2.into()), ("to".into(), 4.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 0.into()), ("to".into(), 2.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 0.into()), ("to".into(), 1.into())],
        ),
        Fact::new(
            "path".into(),
            vec![("from".into(), 1.into()), ("to".into(), 4.into())],
        ),
    ]);

    assert_eq!(
        serde_wasm_bindgen::from_value::<BTreeSet<Fact>>(results).unwrap(),
        expected
    );
}

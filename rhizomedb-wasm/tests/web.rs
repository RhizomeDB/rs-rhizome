#![cfg(target_arch = "wasm32")]

//! Test suite for the Web and headless browsers.

use wasm_bindgen_test::{wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

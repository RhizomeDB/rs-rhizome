use once_cell::sync::Lazy;
use std::sync::Mutex;
use string_interner::{DefaultSymbol, StringInterner};

static INSTANCE: Lazy<Mutex<StringInterner>> = Lazy::new(|| Mutex::new(StringInterner::default()));

pub type Symbol = DefaultSymbol;

pub fn get_or_intern(s: &str) -> Symbol {
    INSTANCE.lock().unwrap().get_or_intern(s)
}

pub fn resolve(s: Symbol) -> String {
    INSTANCE.lock().unwrap().resolve(s).unwrap().to_string()
}

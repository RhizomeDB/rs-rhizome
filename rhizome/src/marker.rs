use std::fmt::Debug;

pub trait SourceMarker: Clone + Eq + PartialEq {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EDB;
impl SourceMarker for EDB {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IDB;
impl SourceMarker for IDB {}

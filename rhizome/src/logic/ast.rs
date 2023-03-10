// TODO: declaration probably shouldn't be pub
pub mod declaration;
pub mod program;

pub(super) mod body_term;
pub(super) mod cid_value;
pub(super) mod clause;
pub(super) mod col;
pub(super) mod col_val;
pub(super) mod dependency;
pub(super) mod fact;
pub(super) mod rule;
pub(super) mod stratum;
pub(super) mod var;

pub use body_term::*;
pub use cid_value::*;
pub use clause::*;
pub use col::*;
pub use col_val::*;
pub use declaration::*;
pub use dependency::*;
pub use fact::*;
pub use program::*;
pub use rule::*;
pub use stratum::*;
pub use var::*;

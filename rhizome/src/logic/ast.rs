// TODO: declaration probably shouldn't be pub
pub mod declaration;
pub mod program;

pub(super) mod body_term;
pub(super) mod cid_value;
pub(super) mod clause;
pub(super) mod column;
pub(super) mod column_value;
pub(super) mod dependency;
pub(super) mod fact;
pub(super) mod rule;
pub(super) mod stratum;
pub(super) mod var;

pub use body_term::*;
pub use cid_value::*;
pub use clause::*;
pub use column::*;
pub use column_value::*;
pub use declaration::*;
pub use dependency::*;
pub use fact::*;
pub use program::*;
pub use rule::*;
pub use stratum::*;
pub use var::*;

#[cfg(test)]
mod tests {
    // use pretty_assertions::assert_eq;

    // use crate::{
    //     error::Error,
    //     id::{ColumnId, RelationId, VariableId},
    //     logic::ast::{attribute_value::AttributeValue, body_term::BodyTerm, rule::Rule},
    // };

    // #[test]
    // fn test_range_restriction() {
    //     assert_eq!(
    //         Some(&Error::RuleNotRangeRestricted(
    //             ColumnId::new("p0"),
    //             VariableId::new("X")
    //         )),
    //         Rule::new(
    //             RelationId::new("p"),
    //             [("p0", AttributeValue::Variable(VariableId::new("X"))],
    //             [BodyTerm::negation(
    //                 "q",
    //                 [("q0", AttributeValue::variable("X"))]
    //             )],
    //         )
    //         .downcast_ref(),
    //     );

    //     assert!(matches!(
    //         Rule::new(
    //             RelationId::new("p"),
    //             [("p0", AttributeValue::variable(VariableId::new("X")))],
    //             [
    //                 BodyTerm::predicate("t", [("t", AttributeValue::variable(VariableId::new("X")))]),
    //                 BodyTerm::negation("q", [("q0", AttributeValue::variable(VariableId::new("X")))]),
    //             ],
    //         ),
    //         Ok(_)
    //     ),);
    // }

    // #[test]
    // fn test_domain_independence() {
    //     assert_eq!(
    //         Some(&Error::RuleNotDomainIndependent(
    //             RelationId::new("q"),
    //             ColumnId::new("q0"),
    //             VariableId::new("X")
    //         )),
    //         Rule::new(
    //             RelationId::new("p"),
    //             [("p0", AttributeValue::variable("P"))],
    //             [
    //                 BodyTerm::predicate("t", [("t0", AttributeValue::variable("P"))]),
    //                 BodyTerm::negation("q", [("q0", AttributeValue::variable("X"))]),
    //             ],
    //         )
    //         .unwrap_err()
    //         .downcast_ref(),
    //     );

    //     assert!(matches!(
    //         Rule::new(
    //             RelationId::new("p"),
    //             [("p0", AttributeValue::variable("X"))],
    //             [
    //                 BodyTerm::predicate("t", [("t0", AttributeValue::variable("X"))]),
    //                 BodyTerm::negation("q", [("q0", AttributeValue::variable("X"))]),
    //             ],
    //         ),
    //         Ok(_)
    //     ),);
    // }
}

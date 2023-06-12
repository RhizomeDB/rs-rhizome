use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};

use crate::{
    error::{error, Error},
    id::RelationId,
    relation::Source,
};

use super::ast::{clause::Clause, program::Program, stratum::Stratum, BodyTerm, Declaration, Rule};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum Node {
    Edb(RelationId),
    Idb(RelationId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum Edge {
    FromEDB(RelationId, RelationId, Polarity),
    FromIDB(RelationId, RelationId, Polarity),
}

impl Edge {
    fn from(&self) -> Node {
        match *self {
            Edge::FromEDB(from, _, _) => Node::Edb(from),
            Edge::FromIDB(from, _, _) => Node::Idb(from),
        }
    }

    fn to(&self) -> Node {
        match *self {
            Edge::FromEDB(_, to, _) => Node::Idb(to),
            Edge::FromIDB(_, to, _) => Node::Idb(to),
        }
    }

    fn polarity(&self) -> Polarity {
        match *self {
            Edge::FromEDB(_, _, polarity) => polarity,
            Edge::FromIDB(_, _, polarity) => polarity,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum Polarity {
    Positive,
    Negative,
}

impl Polarity {
    #[allow(dead_code)]
    fn is_positive(&self) -> bool {
        matches!(self, Polarity::Positive)
    }

    #[allow(dead_code)]
    fn is_negative(&self) -> bool {
        matches!(self, Polarity::Negative)
    }
}

pub(crate) fn stratify(program: &Program) -> Result<Vec<Stratum<'_>>> {
    let mut clauses_by_relation = im::HashMap::<RelationId, im::Vector<&Clause>>::default();

    for clause in program.clauses() {
        clauses_by_relation = clauses_by_relation.alter(
            |old| match old {
                Some(clauses) => {
                    let mut new = clauses;
                    new.push_back(clause);

                    Some(new)
                }
                None => Some(im::vector![clause]),
            },
            clause.head(),
        );
    }

    let mut edg = DiGraph::<Node, Polarity>::default();
    let mut nodes = im::HashMap::<Node, NodeIndex>::default();

    for clause in program.clauses() {
        nodes
            .entry(Node::Idb(clause.head()))
            .or_insert_with(|| edg.add_node(Node::Idb(clause.head())));

        for dependency in clause_depends_on(clause) {
            nodes
                .entry(dependency.to())
                .or_insert_with(|| edg.add_node(dependency.to()));

            nodes
                .entry(dependency.from())
                .or_insert_with(|| edg.add_node(dependency.from()));

            let to = nodes
                .get(&dependency.to())
                .ok_or_else(|| Error::InternalRhizomeError("dependency not found".to_owned()))?;

            let from = nodes
                .get(&dependency.from())
                .ok_or_else(|| Error::InternalRhizomeError("dependency not found".to_owned()))?;

            edg.add_edge(*from, *to, dependency.polarity());
        }
    }

    let sccs = petgraph::algo::kosaraju_scc(&edg);

    for scc in &sccs {
        for node in scc {
            for edge in edg.edges_directed(*node, Direction::Outgoing) {
                if edge.weight().is_negative() && scc.contains(&edge.target()) {
                    return error(Error::ProgramUnstratifiable);
                }
            }
        }
    }

    Ok(sccs
        .iter()
        .map(|nodes| {
            let mut relations: HashSet<RelationId> = HashSet::default();
            let mut clauses: Vec<&Clause> = Vec::default();

            for i in nodes {
                if let Some(Node::Idb(id)) = edg.node_weight(*i) {
                    relations.insert(*id);

                    if let Some(by_relation) = clauses_by_relation.get(id) {
                        for clause in by_relation {
                            clauses.push(*clause);
                        }
                    }
                }
            }

            Stratum::new(
                relations,
                clauses,
                nodes.len() > 1 || edg.contains_edge(nodes[0], nodes[0]),
            )
        })
        .rev()
        .collect())
}

fn clause_depends_on(clause: &Clause) -> Vec<Edge> {
    match clause {
        Clause::Fact(_) => vec![],
        Clause::Rule(rule) => rule_depends_on(rule),
    }
}

fn rule_depends_on(rule: &Rule) -> Vec<Edge> {
    let mut edges = Vec::default();

    for term in rule.body() {
        if let Some(polarity) = term_polarity(term) {
            for dependency in term_depends_on(term) {
                let edge = match dependency.source() {
                    Source::Edb => Edge::FromEDB(dependency.id(), rule.head(), polarity),
                    Source::Idb => Edge::FromIDB(dependency.id(), rule.head(), polarity),
                };

                edges.push(edge);
            }
        }
    }

    edges
}

fn term_polarity(term: &BodyTerm) -> Option<Polarity> {
    match term {
        BodyTerm::RelPredicate(_) => Some(Polarity::Positive),
        BodyTerm::Negation(_) => Some(Polarity::Negative),
        BodyTerm::GetLink(_) => None,
        BodyTerm::VarPredicate(_) => None,
        BodyTerm::Aggregation(_) => Some(Polarity::Negative),
    }
}

fn term_depends_on(term: &BodyTerm) -> Vec<Arc<Declaration>> {
    match term {
        BodyTerm::RelPredicate(inner) => vec![inner.relation()],
        BodyTerm::Negation(inner) => vec![inner.relation()],
        BodyTerm::GetLink(_) => vec![],
        BodyTerm::VarPredicate(_) => vec![],
        BodyTerm::Aggregation(inner) => vec![inner.relation()],
    }
}

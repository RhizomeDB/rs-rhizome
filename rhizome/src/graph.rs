use petgraph::{
    algo::Cycle,
    graphmap::{DiGraphMap, NodeTrait, Nodes},
    Direction, IntoWeightedEdge,
};

#[derive(Clone, Debug)]
pub struct DiGraph<N>
where
    N: NodeTrait,
{
    inner: DiGraphMap<N, ()>,
}

impl<N> Default for DiGraph<N>
where
    N: NodeTrait,
{
    fn default() -> Self {
        Self {
            inner: DiGraphMap::<N, ()>::default(),
        }
    }
}

impl<N> DiGraph<N>
where
    N: NodeTrait,
{
    pub fn from_edges<I>(iterable: I) -> Self
    where
        I: IntoIterator,
        I::Item: IntoWeightedEdge<(), NodeId = N>,
    {
        Self {
            inner: DiGraphMap::<N, ()>::from_edges(iterable),
        }
    }

    pub fn nodes(&self) -> Nodes<'_, N> {
        self.inner.nodes()
    }

    pub fn edges(&self) -> impl Iterator<Item = (N, N)> + '_ {
        self.inner.all_edges().map(|(src, dst, _)| (src, dst))
    }

    pub fn add_edge(&mut self, src: N, dst: N) {
        self.inner.add_edge(src, dst, ());
    }

    pub fn topsort(&self) -> Result<Vec<N>, Cycle<N>> {
        petgraph::algo::toposort(&self.inner, None)
    }

    pub fn sccs(&self) -> Vec<Self> {
        let condensation =
            petgraph::algo::condensation(self.inner.clone().into_graph::<u32>(), true);

        condensation
            .node_weights()
            .map(|nodes| {
                let mut scc = Self::default();

                for node in nodes {
                    let out_edges = self.inner.edges_directed(*node, Direction::Outgoing);

                    for (src, dst, _) in out_edges {
                        if nodes.contains(&dst) {
                            scc.add_edge(src, dst);
                        }
                    }
                }

                scc
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn topsort_tests() {
        // https://upload.wikimedia.org/wikipedia/commons/0/03/Directed_acyclic_graph_2.svg
        let edges = [
            (5, 11),
            (11, 2),
            (11, 9),
            (11, 10),
            (7, 11),
            (7, 8),
            (8, 9),
            (3, 8),
            (3, 10),
        ];

        let position_by_node: BTreeMap<_, _> = DiGraph::<i32>::from_edges(&edges)
            .topsort()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(idx, &node)| (node, idx))
            .collect();

        for (src, dst) in edges {
            assert!(position_by_node[&src] < position_by_node[&dst]);
        }
    }

    #[test]
    fn sccs_tests() {
        // https://en.wikipedia.org/wiki/Strongly_connected_component#/media/File:Scc-1.svg
        let edges = [
            ("a", "b"),
            ("b", "f"),
            ("e", "f"),
            ("e", "a"),
            ("b", "e"),
            ("b", "c"),
            ("c", "g"),
            ("f", "g"),
            ("g", "f"),
            ("c", "d"),
            ("d", "c"),
            ("d", "h"),
            ("h", "d"),
            ("h", "g"),
        ];

        let g = DiGraph::<&str>::from_edges(edges);
        let sccs = g.sccs();

        assert_eq!(3, sccs.len());
        let s0 = &sccs[0];
        let s1 = &sccs[1];
        let s2 = &sccs[2];

        assert_eq!(vec!["f", "g"], s0.nodes().collect::<Vec<_>>());
        assert_eq!(vec![("f", "g"), ("g", "f")], s0.edges().collect::<Vec<_>>());

        assert_eq!(vec!["c", "d", "h"], s1.nodes().collect::<Vec<_>>());
        assert_eq!(
            vec![("c", "d"), ("d", "c"), ("d", "h"), ("h", "d")],
            s1.edges().collect::<Vec<_>>()
        );

        assert_eq!(vec!["a", "b", "e"], s2.nodes().collect::<Vec<_>>());
        assert_eq!(
            vec![("a", "b"), ("b", "e"), ("e", "a")],
            s2.edges().collect::<Vec<_>>()
        );
    }
}

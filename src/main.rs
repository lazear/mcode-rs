pub mod adjacency;
pub mod disjoint;
use disjoint::{DisjointSet, Element};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::io::prelude::*;
use std::iter::FromIterator;

struct Counter<'s> {
    pub counts: HashMap<&'s str, usize>,
}

impl<'s> FromIterator<&'s str> for Counter<'s> {
    fn from_iter<I: IntoIterator<Item = &'s str>>(iter: I) -> Self {
        let mut counts = HashMap::new();
        for i in iter {
            *counts.entry(i).or_insert(0) += 1;
        }
        Counter { counts }
    }
}

#[derive(Default, Debug)]
struct Network<'s> {
    set: DisjointSet<u32>,
    map: HashMap<&'s str, Element>,
    n: u32,
}

impl<'s> Network<'s> {
    pub fn add(&mut self, s: &'s str) -> Element {
        match self.map.get(s) {
            Some(idx) => *idx,
            None => {
                let e = self.set.singleton(self.n);
                self.n += 1;
                self.map.insert(s, e);
                e
            }
        }
    }

    pub fn join(&mut self, a: Element, b: Element) {
        // arbitrarily pick the left element, doesn't really matter
        self.set.union(|a, _| a, a, b)
    }

    pub fn count(&self) -> usize {
        self.set.len()
    }

    pub fn iter(&self) -> NetworkIter<'_, 's> {
        NetworkIter {
            net: self.map.iter(),
            set: &self.set,
        }
    }
}

pub struct NetworkIter<'a, 's> {
    net: std::collections::hash_map::Iter<'a, &'s str, Element>,
    set: &'a DisjointSet<u32>,
}

impl<'a, 's> Iterator for NetworkIter<'a, 's> {
    type Item = (&'s str, u32);
    fn next(&mut self) -> Option<Self::Item> {
        self.net.next().map(|(k, v)| (*k, *self.set.find(*v)))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct EdgeIx(u32);

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct NodeIx(u32);

#[derive(Copy, Clone, Debug)]
pub struct Edge {
    a: NodeIx,
    b: NodeIx,
    w: u16,
}

#[derive(Debug)]
pub struct Node<'s> {
    id: &'s str,
    edges: Vec<EdgeIx>,
}

#[derive(Default, Debug)]
pub struct Graph<'s> {
    map: HashMap<&'s str, NodeIx>,
    nodes: Vec<Node<'s>>,
    edges: Vec<Edge>,
}

impl<'s> Graph<'s> {
    pub fn with_capacity(sz: usize) -> Graph<'s> {
        Graph {
            map: HashMap::with_capacity(sz),
            nodes: Vec::with_capacity(sz),
            edges: Vec::with_capacity(sz),
        }
    }

    pub fn add_node(&mut self, id: &'s str) -> NodeIx {
        match self.map.get(&id).copied() {
            Some(idx) => idx,
            None => {
                let idx = NodeIx(self.nodes.len() as u32);
                self.map.insert(id, idx);
                self.nodes.push(Node {
                    id,
                    edges: Vec::new(),
                });
                idx
            }
        }
    }

    pub fn add_edge(&mut self, a: &'s str, b: &'s str, w: u16) -> EdgeIx {
        let ix = EdgeIx(self.edges.len() as u32);
        let a = self.add_node(a);
        let b = self.add_node(b);
        self.nodes[a.0 as usize].edges.push(ix);
        self.nodes[b.0 as usize].edges.push(ix);
        self.edges.push(Edge { a, b, w });
        ix
    }

    /// Perform a BFS search, visiting nodes up to `depth` links away from the root
    /// Return a set of visited NodeIx's
    pub fn bfs(&self, node: NodeIx, mut depth: usize) -> HashSet<NodeIx> {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        queue.push_back(node);

        while let Some(node_id) = queue.pop_front() {
            visited.insert(node_id);
            let root = self.node(node_id);
            for edge in root.edges.iter().map(|ix| self.edge(*ix)) {
                if visited.insert(edge.a) {
                    queue.push_back(edge.a);
                }

                if visited.insert(edge.b) {
                    queue.push_back(edge.b);
                }
            }
            depth = depth.saturating_sub(1);
            if depth == 0 {
                break;
            }
        }
        visited
    }

    pub fn subgraph(&self, node: NodeIx) -> Graph<'_> {
        let mut g = Graph::default();
        let set = self.bfs(node, 0);
        let mut seen = HashSet::new();
        for &node_id in &set {
            let node = self.node(node_id);
            for &edge_ix in &node.edges {
                let edge = self.edge(edge_ix);
                if set.contains(&edge.a) && set.contains(&edge.b) {
                    let na = self.node(edge.a);
                    let nb = self.node(edge.b);
                    let ga = g.add_node(na.id);
                    let gb = g.add_node(nb.id);
                    if seen.insert((ga, gb)) && seen.insert((gb, ga)) {
                        g.add_edge(na.id, nb.id, edge.w);
                    }
                }
            }
        }

        g
    }

    pub fn node(&self, ix: NodeIx) -> &Node {
        &self.nodes[ix.0 as usize]
    }

    pub fn node_edges(&self, ix: NodeIx) -> EdgeIter<'_, 's> {
        EdgeIter {
            graph: &self,
            edges: self.node(ix).edges.as_slice(),
            idx: 0,
            root: ix,
        }
    }

    pub fn edge(&self, ix: EdgeIx) -> &Edge {
        &self.edges[ix.0 as usize]
    }

    pub fn connected(&self, a: NodeIx, b: NodeIx, depth: usize) -> bool {
        let reach_a = self.bfs(a, depth.saturating_sub(1));
        let reach_b = self.bfs(b, depth.saturating_sub(1));
        reach_a.contains(&b) || reach_b.contains(&a)
    }

    pub fn direct_connection(&self, root: NodeIx, edge: NodeIx) -> Option<u16> {
        for &Edge { a, b, w } in self.node(root).edges.iter().map(|ix| self.edge(*ix)) {
            if a == edge || b == edge {
                return Some(w);
            }
        }
        None
    }

    pub fn graphviz(&self) {
        println!("graph G {{");
        for edge in &self.edges {
            let a = self.node(edge.a);
            let b = self.node(edge.b);
            println!("\t{} -- {}", a.id, b.id);
        }
        println!("}}");
    }

    pub fn csv(&self) {
        for edge in &self.edges {
            let a = self.node(edge.a);
            let b = self.node(edge.b);
            println!("{},{},{}", a.id, b.id, edge.w);
        }
    }

    fn density(&self) -> usize {
        let v = self.nodes.len();
        let v = v * (v - 1) / 2;
        self.edges.len().checked_div(v).unwrap_or(0)
    }

    fn kcore(&self) -> (usize, Graph<'_>) {
        let mut retain = (0..self.nodes.len()).collect::<HashSet<usize>>();
        let mut degrees = self.nodes.iter().map(|n| n.edges.len()).collect::<Vec<_>>();
        let mut k = 2;

        let (k, nodes) = loop {
            let mut remove = Vec::new();
            retain = retain
                .drain()
                .filter(|&idx| {
                    if degrees[idx] < k {
                        degrees[idx] = 0;
                        remove.push(idx);
                        false
                    } else {
                        true
                    }
                })
                .collect();

            if retain.is_empty() {
                break (k - 1, remove);
            }

            for idx in remove {
                let node = &self.nodes[idx];
                for &edge in &node.edges {
                    let e = self.edge(edge);
                    degrees[e.a.0 as usize] = degrees[e.a.0 as usize].saturating_sub(1);
                    degrees[e.b.0 as usize] = degrees[e.b.0 as usize].saturating_sub(1);
                }
            }
            k += 1;
        };

        let mut g = Graph::default();
        let mut s = HashSet::new();
        for &node_id in &nodes {
            for edge in &self.nodes[node_id].edges {
                let edge = self.edge(*edge);
                if nodes.contains(&(edge.a.0 as usize)) && nodes.contains(&(edge.b.0 as usize)) {
                    let na = self.node(edge.a);
                    let nb = self.node(edge.b);
                    let ga = g.add_node(na.id);
                    let gb = g.add_node(nb.id);
                    if s.insert((ga, gb)) && s.insert((gb, ga)) {
                        g.add_edge(na.id, nb.id, edge.w);
                    }
                }
            }
        }
        (k, g)
    }

    // fn kcore2(&self, start: NodeIx) -> (usize, Graph<'_>) {
    //     let root = self.node(start);
    //     let mut retain = self.node_edges(start).enumerate().map(|(ix, ni)| (ix, ni)).collect::<Vec<_>>();
    //     let mut degrees = self.node_edges(start).map(|ni| self.node(ni).edges.len()).collect::<Vec<usize>>();
    //     let mut k = 2;

    //     let (k, nodes) = loop {
    //         let mut remove = Vec::new();
    //         retain = retain
    //             .drain(..)
    //             .filter(|&(idx, node_ix)| {
    //                 if degrees[idx] < k {
    //                     degrees[idx] = 0;
    //                     remove.push((idx, node_ix));
    //                     false
    //                 } else {
    //                     true
    //                 }
    //             })
    //             .collect();

    //         if retain.is_empty() {
    //             break (k - 1, remove);
    //         }

    //         for (idx, nix) in remove {
    //             let node = self.node(nix);
    //             for &edge in &node.edges {
    //                 let e = self.edge(edge);
    //                 degrees[e.a.0 as usize] = degrees[e.a.0 as usize].saturating_sub(1);
    //                 degrees[e.b.0 as usize] = degrees[e.b.0 as usize].saturating_sub(1);
    //             }
    //         }
    //         k += 1;
    //     };

    //     let mut g = Graph::default();
    //     let mut s = HashSet::new();
    //     for &(ix, node_id) in &nodes {
    //         for &edge in &self.node(node_id).edges {
    //             let edge = self.edge(edge);
    //             if nodes.contains(&(ix, edge.a)) && nodes.contains(&(ix, edge.b)) {
    //                 let na = self.node(edge.a);
    //                 let nb = self.node(edge.b);
    //                 let ga = g.add_node(na.id);
    //                 let gb = g.add_node(nb.id);
    //                 if s.insert((ga, gb)) && s.insert((gb, ga)) {
    //                     g.add_edge(na.id, nb.id, edge.w);
    //                 }
    //             }
    //         }
    //     }
    //     (k, g)
    // }

    fn weight(&self) -> usize {
        let (k, kg) = self.kcore();
        let dens = kg.density();
        k * dens
    }
}

pub struct EdgeIter<'g, 's> {
    graph: &'g Graph<'s>,
    edges: &'g [EdgeIx],
    idx: usize,
    root: NodeIx,
}

impl<'g, 's> Iterator for EdgeIter<'g, 's> {
    type Item = NodeIx;
    fn next(&mut self) -> Option<Self::Item> {
        let e = self.graph.edge(*self.edges.get(self.idx)?);
        self.idx += 1;
        if e.a == self.root {
            Some(e.b)
        } else {
            Some(e.a)
        }
    }
}

fn main() -> io::Result<()> {
    let mut f = std::fs::File::open("data/cleaned.csv")?;
    // let mut f = std::fs::File::open("out.csv")?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;

    let mut g = Graph::with_capacity(20_000);
    for line in buffer.lines().skip(1) {
        let mut iter = line.split(',');
        let a = iter.next().unwrap();
        let b = iter.next().unwrap();
        let w = iter.next().unwrap().parse::<u16>().unwrap();
        if a == "unknown" || b == "unknown" {
            continue;
        }
        g.add_edge(a, b, w);
    }

    // let mut g = g.subgraph(g.map["CCND1"]);

    for i in 0..g.nodes.len() {
        let k = g.subgraph(NodeIx(i as u32));
        // let w = g.kcore().1.weight();
        let (k, gg) = k.kcore();
        // println!("{} {} {}", g.nodes[i].id, k, gg.weight());
    }

    // g.csv();
    // let (k, g) = g.kcore2(g.map["PIN1"]);
    // g.graphviz();
    // dbg!(g.weight());

    Ok(())
}

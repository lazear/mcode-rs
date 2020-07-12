pub mod disjoint;
use disjoint::DisjointSet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::io::prelude::*;

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

    pub fn neighbors(&self, ix: NodeIx) -> Neighbors<'_, 's> {
        Neighbors {
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

    fn density(&self) -> f32 {
        let mut v = self.nodes.len() as f32;
        v *= v - 1.0;
        if v == 0.0 {
            v = 1.0;
        }
        2.0 * self.edges.len() as f32 / v
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

    fn weight(&self) -> f32 {
        let (k, kg) = self.kcore();
        let dens = kg.density();
        k as f32 * dens
    }
}

pub struct Neighbors<'g, 's> {
    graph: &'g Graph<'s>,
    edges: &'g [EdgeIx],
    idx: usize,
    root: NodeIx,
}

impl<'g, 's> Iterator for Neighbors<'g, 's> {
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

/// Cache intermediate results (k-core weights) in a file for faster testing
fn read_or_generate_weights<P: AsRef<std::path::Path>>(
    path: P,
    graph: &Graph<'_>,
) -> io::Result<HashMap<String, f32>> {
    if path.as_ref().exists() {
        let mut f = fs::File::open(path)?;
        let mut weight_buffer = String::new();
        f.read_to_string(&mut weight_buffer)?;
        let mut weights = HashMap::new();
        for line in weight_buffer.lines() {
            let mut iter = line.split(' ');
            let v = iter.next().unwrap();
            let w = iter
                .next()
                .unwrap()
                .replace("NaN", "0.0")
                .parse::<f32>()
                .unwrap();
            weights.insert(v.into(), w);
        }
        Ok(weights)
    } else {
        let mut f = fs::File::create(path)?;
        let mut weights = HashMap::new();
        for i in 0..graph.nodes.len() {
            let k = graph.subgraph(NodeIx(i as u32));
            weights.insert(graph.nodes[i].id.into(), k.weight());
        }

        for (k, v) in &weights {
            writeln!(f, "{} {}", k, v)?;
        }

        Ok(weights)
    }
}

/// Pick a seed protein
fn pick_seed(weights: &HashMap<String, f32>) -> &str {
    let mut best = weights.iter().next().unwrap();
    for (k, v) in weights {
        if *v > *best.1 {
            best = (k, v);
        }
    }
    best.0
}

/// use the MCODE algorithm to assign a protein to a complex
fn assign_complex<'s>(
    graph: &Graph<'s>,
    weights: &HashMap<String, f32>,
    density: f32,
) -> HashMap<&'s str, NodeIx> {
    let mut membership = HashMap::new();
    let mut complex_set = DisjointSet::new();
    let mut stack = Vec::new();
    let mut visited = HashSet::new();

    let seed = graph.map[pick_seed(weights)];
    stack.push(seed);

    for ix in (0..graph.nodes.len() as u32).map(NodeIx) {
        membership.insert(ix, complex_set.singleton(ix));
    }

    // save the last unvisited node id, so that we can traverse linearly
    let mut ptr = NodeIx(0);
    // outer loop, while we haven't visited every node in the graph
    while visited.len() != graph.nodes.len() {
        // depth-first traversal, starting from seed node
        while let Some(nix) = stack.pop() {
            visited.insert(nix);
            let node = graph.node(nix);
            for neighbor_ix in graph.neighbors(nix) {
                let neighbor = graph.node(neighbor_ix);
                if visited.insert(neighbor_ix) {
                    if weights[neighbor.id] > (weights[node.id] * (1.0 - density)) {
                        complex_set.union(|a, _| a, membership[&nix], membership[&neighbor_ix]);
                    }
                    stack.push(neighbor_ix);
                }
            }
        }

        for ix in (ptr.0..graph.nodes.len() as u32).map(NodeIx) {
            if !visited.contains(&ix) {
                ptr = ix;
                break;
            }
        }
        stack.push(ptr);
    }

    let mut complexes = HashMap::new();
    for (ix, node) in graph.nodes.iter().enumerate() {
        let element = membership[&NodeIx(ix as u32)];
        complexes.insert(node.id, *complex_set.find(element));
    }

    complexes
}

fn main() -> io::Result<()> {
    let mut f = fs::File::open("data/cleaned.csv")?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;

    let mut g = Graph::with_capacity(25_000);
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

    let weights = read_or_generate_weights("weights", &g)?;
    let map = assign_complex(&g, &weights, 0.8);
    let mut out = fs::File::create("output.tsv")?;

    for (k, v) in map {
        write!(out, "{}\t{}", k, v.0)?;
    }

    Ok(())
}

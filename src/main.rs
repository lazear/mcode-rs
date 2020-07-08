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

        for &node_id in &set {
            let node = self.node(node_id);
            for &edge_ix in &node.edges {
                let edge = self.edge(edge_ix);
                if set.contains(&edge.a) && set.contains(&edge.b) {
                    let na = self.node(edge.a);
                    let nb = self.node(edge.b);
                    let ga = g.add_node(na.id);
                    let gb = g.add_node(nb.id);
                    if g.direct_connection(ga, gb).is_some()
                        || g.direct_connection(gb, ga).is_some()
                    {
                        continue;
                    }
                    g.add_edge(na.id, nb.id, edge.w);
                }
            }
        }

        g
    }

    pub fn node(&self, ix: NodeIx) -> &Node {
        &self.nodes[ix.0 as usize]
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
}

fn main() -> io::Result<()> {
    let mut f = std::fs::File::open("data/cleaned.csv")?;
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

    dbg!(g.nodes.len());
    dbg!(g.edges.len());

    // dbg!(g
    //     .bfs(g.map["Q13526"], 2)
    //     .into_iter()
    //     .map(|ix| g.nodes[ix.0 as usize].id)
    //     .collect::<Vec<&str>>());

    g.bfs(g.map["Q13526"], 1);

    let sub = g.subgraph(g.map["O95302"]);
    dbg!(sub.nodes.len());
    dbg!(sub.edges.len());
    for node in &sub.nodes {
        // println!("{} {}", node.id, node.edges.len());
        for edge in &node.edges {
            let a = sub.node(sub.edge(*edge).a);
            let b = sub.node(sub.edge(*edge).b);
            let id = if a.id == node.id { b.id } else { a.id };
            println!("{} {} -> {}", node.edges.len(), node.id, id);
        }
    }

    // let mut weights = HashMap::new();

    // for node in 0..g.nodes.len() {
    //     if (node % 1000) == 0 {
    //         eprintln!("{}", node);
    //     }
    //     let set = g.bfs(NodeIx(node as u32), 1);
    //     weights.insert(g.nodes[node].id, set.len());
    // }

    // let mut w = weights.into_iter().collect::<Vec<_>>();
    // w.sort_by(|a, b| a.1.cmp(&b.1));

    // for (n, s) in w {
    //     println!("{} {}", n, s);
    // }

    // let c = prots.iter().copied().collect::<Counter>();
    // let mut net = Network::default();

    // for slice in prots.chunks(2) {
    //     // if c.counts[slice[0]] > 5 || c.counts[slice[1]] > 5 {
    //     //     continue;
    //     // }
    //     let a = net.add(slice[0]);
    //     let b = net.add(slice[1]);
    //     net.join(a, b);
    // }

    // dbg!(net.count());

    // let mut out = std::fs::File::create("networks.csv")?;
    // for (s, network_id) in net.iter() {
    //     writeln!(out, "{},{}", s, network_id)?;
    // }

    Ok(())
}
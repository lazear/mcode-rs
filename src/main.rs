pub mod disjoint;
use disjoint::{Element, DisjointSet};
use std::io;
use std::io::prelude::*;
use std::collections::HashMap;
use std::iter::FromIterator;


struct Counter<'s> {
    pub counts: HashMap<&'s str, usize>
}

impl<'s> FromIterator<&'s str> for Counter<'s> {
    fn from_iter<I: IntoIterator<Item = &'s str>>(iter: I) -> Self {
        let mut counts = HashMap::new();
        for i in iter {
            *counts.entry(i).or_insert(0) += 1;
        }
        Counter {
            counts
        }
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
        self.set.union(|a,_| a, a, b)
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

fn main() -> io::Result<()> {
    let mut f = std::fs::File::open("data/cleaned.csv")?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;


    let mut prots = Vec::new();

    for line in buffer.lines().skip(1) {
        let mut iter = line.split(',');
        let a = iter.next().unwrap();
        let b = iter.next().unwrap();
        prots.push(a);
        prots.push(b);
    }

    let c = prots.iter().copied().collect::<Counter>();
    let mut net = Network::default();

    for slice in prots.chunks(2) {
        // if c.counts[slice[0]] > 5 || c.counts[slice[1]] > 5 {
        //     continue;
        // }
        let a = net.add(slice[0]);
        let b = net.add(slice[1]);
        net.join(a, b);
    }   


    dbg!(net.count());

    let mut out = std::fs::File::create("networks.csv")?;
    for (s, network_id) in net.iter() {
        writeln!(out, "{},{}", s, network_id)?;
    }


    Ok(())
}

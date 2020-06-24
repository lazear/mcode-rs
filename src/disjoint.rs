//! A disjoint set using the union-find algorithm with path-compression

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::HashSet;

struct SetElement<T> {
    data: Option<T>,
    rank: Cell<u32>,
    parent: Cell<usize>,
}

pub struct DisjointSet<T> {
    elements: Vec<SetElement<T>>,
    components: Cell<usize>,
}

impl<T> Default for DisjointSet<T> {
    fn default() -> Self {
        DisjointSet {
            elements: Vec::new(),
            components: Cell::new(0),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct Element(usize);

pub enum Choice {
    Left,
    Right,
}

impl<T> DisjointSet<T> {
    pub fn new() -> DisjointSet<T> {
        DisjointSet {
            elements: Vec::new(),
            components: Cell::new(0),
        }
    }

    pub fn singleton(&mut self, data: T) -> Element {
        let n = self.elements.len();
        let elem = SetElement {
            data: Some(data),
            rank: Cell::new(0),
            parent: Cell::new(n),
        };
        self.elements.push(elem);
        self.components.replace(self.components.get() + 1);
        Element(n)
    }

    pub fn singleton_f<F: FnOnce(Element) -> T>(&mut self, f: F) {
        let n = self.elements.len();
        let data = f(Element(n));
        let elem = SetElement {
            data: Some(data),
            rank: Cell::new(0),
            parent: Cell::new(n),
        };
        self.elements.push(elem);
        self.components.replace(self.components.get() + 1);
    }

    fn find_set(&self, id: usize) -> usize {
        // locate parent set
        let mut ptr = id;
        while ptr != self.elements[ptr].parent.get() {
            ptr = self.elements[ptr].parent.get();
        }

        // id is the representative element, return
        if ptr == id {
            return id;
        }

        // perform path compression
        let parent = ptr;
        ptr = id;
        while ptr != self.elements[ptr].parent.get() {
            ptr = self.elements[ptr].parent.replace(parent);
        }
        parent
    }

    pub fn find_repr(&self, element: Element) -> Element {
        Element(self.find_set(element.0))
    }

    pub fn data(&self, element: Element) -> Option<&T> {
        self.elements[element.0].data.as_ref()
    }

    pub fn find(&self, element: Element) -> &T {
        // Invariant that the representative element is always "Some"
        self.elements[self.find_set(element.0)]
            .data
            .as_ref()
            .expect("Invariant violated")
    }

    pub fn union<F: Fn(T, T) -> T>(&mut self, f: F, a: Element, b: Element) {
        let pa = self.find_set(a.0);
        let pb = self.find_set(b.0);

        if pa == pb {
            return;
        }

        // Move data out first to appease borrowck
        let a_data = self.elements[pa].data.take().expect("Invariant violated");
        let b_data = self.elements[pb].data.take().expect("Invariant violated");

        self.components.replace(self.components.get() - 1);
        match self.elements[pa].rank.cmp(&self.elements[pb].rank) {
            Ordering::Equal => {
                self.elements[pa].data = Some(f(a_data, b_data));
                self.elements[pb].parent.replace(pa);
                self.elements[pa].rank.replace(self.elements[pa].rank.get() + 1);
            }
            Ordering::Less => {
                self.elements[pb].data = Some(f(a_data, b_data));
                self.elements[pa].parent.replace(pb);
                self.elements[pb].rank.replace(self.elements[pb].rank.get() + 1);
            }
            Ordering::Greater => {
                self.elements[pa].data = Some(f(a_data, b_data));
                self.elements[pb].parent.replace(pa);
                self.elements[pa].rank.replace(self.elements[pa].rank.get() + 1);
            }
        }
    }

    pub fn partition(&self) -> Vec<&T> {
        let mut v = HashSet::new();

        for idx in 0..self.elements.len() {
            v.insert(self.find_set(idx));
        }
        v.into_iter()
            .map(|idx| self.elements[idx].data.as_ref().unwrap())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.components.get()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for DisjointSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let part = self.partition();
        writeln!(f, "{{")?;
        for values in part {
            write!(f, "\t{:?}\n", values)?;
        }
        writeln!(f, "}}")
    }
}
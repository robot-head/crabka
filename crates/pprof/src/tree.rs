//! Symbolized profile tree and flamegraph encoding.

use std::collections::{HashMap, HashSet};

use crate::Frame;

const ROOT_NAME: &str = "total";
const OTHER_NAME: &str = "other";

/// One flamegraph level. Values are groups of four:
/// `[xOffsetDelta, total, self, nameIndex]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Level {
    pub values: Vec<i64>,
}

/// Pyroscope-compatible single flamegraph projection.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraph {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub total: i64,
    pub max_self: i64,
}

/// Diff flamegraph type frozen for the next slice.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraphDiff {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub left_ticks: i64,
    pub right_ticks: i64,
}

#[derive(Clone, Debug)]
struct Node {
    name: String,
    total: i64,
    self_: i64,
    children: Vec<usize>,
    child_by_name: HashMap<String, usize>,
}

/// Symbolized profile tree. Root is the synthetic `"total"` node.
#[derive(Clone, Debug)]
pub struct Tree {
    nodes: Vec<Node>,
    root: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeSnapshotNode {
    pub name: String,
    pub total: i64,
    pub self_: i64,
    pub children: Vec<usize>,
}

impl Tree {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: vec![Node {
                name: ROOT_NAME.to_string(),
                total: 0,
                self_: 0,
                children: Vec::new(),
                child_by_name: HashMap::new(),
            }],
            root: 0,
        }
    }

    pub fn add_stack(&mut self, frames: &[Frame], value: i64) {
        // Stackless samples (a goroutine profile occasionally emits one with no
        // frames) carry no position in the flamegraph, so they contribute
        // nothing to the tree — matching Pyroscope, which keeps them in series
        // sums but excludes them from flamegraph/merge-stacktrace totals.
        if frames.is_empty() {
            return;
        }
        let mut current = self.root;
        self.nodes[current].total += value;
        for frame in frames.iter().rev() {
            let name = frame.function.clone();
            let child = if let Some(child) = self.nodes[current].child_by_name.get(&name) {
                *child
            } else {
                let idx = self.nodes.len();
                self.nodes.push(Node {
                    name: name.clone(),
                    total: 0,
                    self_: 0,
                    children: Vec::new(),
                    child_by_name: HashMap::new(),
                });
                let pos = sorted_child_position(&self.nodes[current].children, &self.nodes, &name);
                self.nodes[current].children.insert(pos, idx);
                self.nodes[current].child_by_name.insert(name, idx);
                idx
            };
            current = child;
            self.nodes[current].total += value;
        }
        self.nodes[current].self_ += value;
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn merge(&mut self, other: Tree) {
        self.merge_node(self.root, &other, other.root);
    }

    fn merge_node(&mut self, target: usize, other: &Tree, source: usize) {
        self.nodes[target].total += other.nodes[source].total;
        self.nodes[target].self_ += other.nodes[source].self_;
        for source_child in &other.nodes[source].children {
            let name = &other.nodes[*source_child].name;
            let target_child = if let Some(child) = self.nodes[target].child_by_name.get(name) {
                *child
            } else {
                let idx = self.nodes.len();
                self.nodes.push(Node {
                    name: name.clone(),
                    total: 0,
                    self_: 0,
                    children: Vec::new(),
                    child_by_name: HashMap::new(),
                });
                let pos = sorted_child_position(&self.nodes[target].children, &self.nodes, name);
                self.nodes[target].children.insert(pos, idx);
                self.nodes[target].child_by_name.insert(name.clone(), idx);
                idx
            };
            self.merge_node(target_child, other, *source_child);
        }
    }

    #[must_use]
    pub fn to_flamegraph(self, max_nodes: i64) -> FlameGraph {
        let keep = self.keep_set(max_nodes);
        let mut names = vec![ROOT_NAME.to_string()];
        let mut name_index = HashMap::from([(ROOT_NAME.to_string(), 0_i64)]);
        let mut level_bars: Vec<Vec<[i64; 4]>> = Vec::new();
        let mut stack = vec![Bar {
            node: Some(self.root),
            name: ROOT_NAME.to_string(),
            total: self.nodes[self.root].total,
            self_: self.nodes[self.root].self_,
            x_start: 0,
            level: 0,
        }];
        let mut max_self = 0;

        while let Some(bar) = stack.pop() {
            let name_idx = name_slot(&mut names, &mut name_index, &bar.name);
            if bar.level == level_bars.len() {
                level_bars.push(Vec::new());
            }
            level_bars[bar.level].push([bar.x_start, bar.total, bar.self_, name_idx]);
            max_self = max_self.max(bar.self_);

            let mut children = Vec::new();
            append_children(&self, &keep, &bar, &mut children);
            stack.extend(children);
        }

        let levels = level_bars
            .into_iter()
            .map(|mut bars| {
                bars.reverse();
                let mut values = Vec::with_capacity(bars.len() * 4);
                let mut previous_end = 0;
                for [x_start, total, self_, name_idx] in bars {
                    let delta = x_start - previous_end;
                    values.extend([delta, total, self_, name_idx]);
                    previous_end = x_start + total;
                }
                Level { values }
            })
            .collect();

        FlameGraph {
            total: self.nodes[self.root].total,
            names,
            levels,
            max_self,
        }
    }

    #[must_use]
    pub fn to_pyroscope_tree_bytes(self, max_nodes: i64) -> Vec<u8> {
        if self.nodes[self.root].children.is_empty() {
            return Vec::new();
        }
        let keep = self.keep_set(max_nodes);
        let mut out = Vec::new();
        let root = Bar {
            node: Some(self.root),
            name: String::new(),
            total: self.nodes[self.root].total,
            self_: 0,
            x_start: 0,
            level: 0,
        };
        let mut stack = vec![root];
        while let Some(parent) = stack.pop() {
            write_pyroscope_tree_node(&mut out, &parent.name, parent.self_);
            let mut children = Vec::new();
            append_children(&self, &keep, &parent, &mut children);
            write_uvarint(&mut out, children.len() as u64);
            stack.extend(children);
        }
        out
    }

    fn keep_set(&self, max_nodes: i64) -> HashSet<usize> {
        let max_nodes = usize::try_from(max_nodes.max(1)).unwrap_or(usize::MAX);
        if self.nodes.len() <= max_nodes {
            return (0..self.nodes.len()).collect();
        }
        let parents = self.parents();
        let mut ranked: Vec<usize> = (0..self.nodes.len())
            .filter(|node| *node != self.root)
            .collect();
        ranked.sort_by(|left, right| {
            self.nodes[*right]
                .total
                .cmp(&self.nodes[*left].total)
                .then_with(|| self.nodes[*left].name.cmp(&self.nodes[*right].name))
                .then_with(|| left.cmp(right))
        });
        let mut keep = HashSet::from([self.root]);
        for node in ranked {
            let mut path = Vec::new();
            let mut current = Some(node);
            while let Some(idx) = current {
                if keep.contains(&idx) {
                    break;
                }
                path.push(idx);
                current = parents[idx];
            }
            if keep.len() + path.len() <= max_nodes {
                keep.extend(path);
            }
        }
        keep
    }

    fn parents(&self) -> Vec<Option<usize>> {
        let mut parents = vec![None; self.nodes.len()];
        for (parent, node) in self.nodes.iter().enumerate() {
            for child in &node.children {
                parents[*child] = Some(parent);
            }
        }
        parents
    }

    pub(crate) fn snapshot(&self) -> (usize, Vec<TreeSnapshotNode>) {
        (
            self.root,
            self.nodes
                .iter()
                .map(|node| TreeSnapshotNode {
                    name: node.name.clone(),
                    total: node.total,
                    self_: node.self_,
                    children: node.children.clone(),
                })
                .collect(),
        )
    }

    pub(crate) fn sample_paths(&self, max_nodes: i64) -> Vec<(Vec<String>, i64)> {
        let keep = self.keep_set(max_nodes);
        let mut samples = Vec::new();
        let mut path = Vec::new();
        self.collect_sample_paths(self.root, &keep, &mut path, &mut samples);
        samples
    }

    fn collect_sample_paths(
        &self,
        node_idx: usize,
        keep: &HashSet<usize>,
        path: &mut Vec<String>,
        samples: &mut Vec<(Vec<String>, i64)>,
    ) {
        if node_idx != self.root {
            path.push(self.nodes[node_idx].name.clone());
        }
        if self.nodes[node_idx].self_ != 0 && !path.is_empty() {
            samples.push((path.clone(), self.nodes[node_idx].self_));
        }
        let mut other_self = 0;
        for child in &self.nodes[node_idx].children {
            if keep.contains(child) {
                self.collect_sample_paths(*child, keep, path, samples);
            } else {
                other_self += subtree_self(self, *child);
            }
        }
        if other_self != 0 {
            let mut other_path = path.clone();
            other_path.push(OTHER_NAME.to_string());
            samples.push((other_path, other_self));
        }
        if node_idx != self.root {
            path.pop();
        }
    }

    #[cfg(test)]
    fn total_of(&self, path: &[&str]) -> i64 {
        self.node_at(path).total
    }

    #[cfg(test)]
    fn self_of(&self, path: &[&str]) -> i64 {
        self.node_at(path).self_
    }

    #[cfg(test)]
    fn node_at(&self, path: &[&str]) -> &Node {
        assert!(!path.is_empty());
        assert_eq!(path[0], ROOT_NAME);
        let mut idx = self.root;
        for name in &path[1..] {
            idx = *self.nodes[idx]
                .child_by_name
                .get(*name)
                .expect("path exists");
        }
        &self.nodes[idx]
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct Bar {
    node: Option<usize>,
    name: String,
    total: i64,
    self_: i64,
    x_start: i64,
    level: usize,
}

fn append_children(tree: &Tree, keep: &HashSet<usize>, parent: &Bar, next: &mut Vec<Bar>) {
    let Some(parent_node) = parent.node else {
        return;
    };
    let mut x = parent.x_start + parent.self_;
    let mut other_total = 0;
    let mut other_self = 0;
    for child in &tree.nodes[parent_node].children {
        let node = &tree.nodes[*child];
        if keep.contains(child) {
            next.push(Bar {
                node: Some(*child),
                name: node.name.clone(),
                total: node.total,
                self_: node.self_,
                x_start: x,
                level: parent.level + 1,
            });
        } else {
            other_total += node.total;
            other_self += subtree_self(tree, *child);
        }
        x += node.total;
    }
    if other_total > 0 {
        next.push(Bar {
            node: None,
            name: OTHER_NAME.to_string(),
            total: other_total,
            self_: other_self,
            x_start: parent.x_start + parent.total - other_total,
            level: parent.level + 1,
        });
    }
}

fn subtree_self(tree: &Tree, node: usize) -> i64 {
    tree.nodes[node].self_
        + tree.nodes[node]
            .children
            .iter()
            .map(|child| subtree_self(tree, *child))
            .sum::<i64>()
}

fn name_slot(names: &mut Vec<String>, index: &mut HashMap<String, i64>, name: &str) -> i64 {
    if let Some(slot) = index.get(name) {
        return *slot;
    }
    let slot = i64::try_from(names.len()).expect("name index fits i64");
    names.push(name.to_string());
    index.insert(name.to_string(), slot);
    slot
}

fn sorted_child_position(children: &[usize], nodes: &[Node], name: &str) -> usize {
    children
        .binary_search_by(|candidate| nodes[*candidate].name.as_str().cmp(name))
        .unwrap_or_else(|pos| pos)
}

fn write_pyroscope_tree_node(out: &mut Vec<u8>, name: &str, self_: i64) {
    write_uvarint(out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    write_uvarint(out, self_.cast_unsigned());
}

fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    for _ in 0..10 {
        if value < 0x80 {
            out.push(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
            return;
        }
        let low_bits = u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8");
        out.push(low_bits + 0x80);
        value >>= 7;
    }
    unreachable!("u64 uvarint uses at most 10 bytes");
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::Frame;

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    fn stack(names: &[&str]) -> Vec<Frame> {
        names.iter().map(|name| frame(name)).collect()
    }

    #[test]
    fn add_stack_totals_along_path_and_self_at_leaf() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 10);
        tree.add_stack(&stack(&["other", "main"]), 3);

        assert_eq!(tree.total_of(&["total"]), 13);
        assert_eq!(tree.self_of(&["total"]), 0);
        assert_eq!(tree.total_of(&["total", "main"]), 13);
        assert_eq!(tree.self_of(&["total", "main"]), 0);
        assert_eq!(tree.total_of(&["total", "main", "work"]), 10);
        assert_eq!(tree.self_of(&["total", "main", "work"]), 10);
        assert_eq!(tree.self_of(&["total", "main", "other"]), 3);
    }

    #[test]
    fn add_stack_ignores_stackless_samples() {
        // Pyroscope keeps stackless samples in series sums but excludes them
        // from flamegraph totals; a stackless sample must not inflate the tree.
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 10);
        tree.add_stack(&[], 1);

        assert_eq!(tree.total_of(&["total"]), 10);
        assert_eq!(tree.self_of(&["total"]), 0);
        let fg = tree.to_flamegraph(2048);
        assert!(fg.total == 10);
    }

    #[test]
    fn merge_combines_partial_trees() {
        let mut a = Tree::new();
        a.add_stack(&stack(&["work", "main"]), 10);
        let mut b = Tree::new();
        b.add_stack(&stack(&["work", "main"]), 5);
        b.add_stack(&stack(&["new", "main"]), 2);
        a.merge(b);
        assert_eq!(a.total_of(&["total"]), 17);
        assert_eq!(a.total_of(&["total", "main", "work"]), 15);
        assert_eq!(a.self_of(&["total", "main", "new"]), 2);
    }

    #[test]
    fn to_flamegraph_root_level_and_names() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["a", "main"]), 6);
        tree.add_stack(&stack(&["b", "main"]), 4);
        let fg = tree.to_flamegraph(2048);
        assert_eq!(fg.names[0].as_str(), "total");
        assert_eq!(fg.total, 10);
        assert_eq!(&fg.levels[0].values, &vec![0, 10, 0, 0]);
    }

    #[test]
    fn to_flamegraph_xoffset_is_delta_from_previous_bar_end() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["a", "main"]), 6);
        tree.add_stack(&stack(&["b", "main"]), 4);
        let fg = tree.to_flamegraph(2048);
        assert!(fg.levels[1].values[0..4] == [0, 10, 0, names_index(&fg, "main")]);
        let a = &fg.levels[2].values[0..4];
        assert!(a[0] == 0 && a[1] == 6 && a[2] == 6);
        let b = &fg.levels[2].values[4..8];
        assert!(b[0] == 0 && b[1] == 4 && b[2] == 4);
    }

    #[test]
    fn to_flamegraph_places_children_after_parent_self() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["main"]), 5);
        tree.add_stack(&stack(&["work", "main"]), 7);

        let fg = tree.to_flamegraph(2048);
        let work = &fg.levels[2].values[0..4];

        assert!(work == [5, 7, 7, names_index(&fg, "work")]);
    }

    #[test]
    fn to_flamegraph_sorts_siblings_like_pyroscope_function_tree() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["z_leaf", "main"]), 6);
        tree.add_stack(&stack(&["a_leaf", "main"]), 4);

        let fg = tree.to_flamegraph(2048);

        assert_eq!(
            fg.names.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["total", "main", "z_leaf", "a_leaf"]
        );
        assert_eq!(
            &fg.levels[2].values,
            &vec![
                0,
                4,
                4,
                names_index(&fg, "a_leaf"),
                0,
                6,
                6,
                names_index(&fg, "z_leaf"),
            ]
        );
    }

    #[test]
    fn to_flamegraph_truncates_with_synthetic_other() {
        let mut tree = Tree::new();
        for idx in 0..10 {
            tree.add_stack(&stack(&[&format!("leaf{idx}"), "main"]), 1);
        }
        let fg = tree.to_flamegraph(4);
        assert_eq!(fg.names.iter().any(|name| name == "other"), true);
        assert_eq!(fg.total, 10);
    }

    #[test]
    fn to_flamegraph_synthetic_other_keeps_hidden_self_sum() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["hot", "main"]), 10);
        tree.add_stack(&stack(&["cold", "main"]), 5);
        tree.add_stack(&stack(&["warm", "main"]), 4);

        let fg = tree.to_flamegraph(3);
        let other = names_index(&fg, "other");
        let other_bar = fg.levels[2]
            .values
            .as_chunks::<4>()
            .0
            .iter()
            .find(|chunk| chunk[3] == other)
            .unwrap();

        assert_eq!(other_bar[1], 9);
        assert_eq!(other_bar[2], 9);
    }

    #[test]
    fn sample_paths_exclude_internal_zero_self_and_restore_sibling_paths() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["a", "main"]), 7);
        tree.add_stack(&stack(&["b", "main"]), 3);

        let mut samples = tree.sample_paths(2048);
        samples.sort();

        assert!(
            samples
                == vec![
                    (vec!["main".to_string(), "a".to_string()], 7),
                    (vec!["main".to_string(), "b".to_string()], 3),
                ]
        );
    }

    #[test]
    fn to_pyroscope_tree_bytes_uses_virtual_root_and_function_nodes() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 7);

        let bytes = tree.to_pyroscope_tree_bytes(2048);

        assert!(bytes == b"\x00\x00\x01\x04main\x00\x01\x04work\x07\x00");
    }

    #[test]
    fn write_uvarint_encodes_single_and_multi_byte_values() {
        let mut out = Vec::new();

        write_uvarint(&mut out, 0);
        write_uvarint(&mut out, 127);
        write_uvarint(&mut out, 128);
        write_uvarint(&mut out, 300);

        assert!(out == vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);
    }

    fn names_index(fg: &FlameGraph, name: &str) -> i64 {
        i64::try_from(fg.names.iter().position(|n| n == name).unwrap()).unwrap()
    }
}

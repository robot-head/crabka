//! Diff flamegraph alignment.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::tree::{Tree, TreeSnapshotNode};
use crate::{FlameGraphDiff, Level};

const ROOT_NAME: &str = "total";
const OTHER_NAME: &str = "other";

#[derive(Clone, Debug)]
struct MergedNode {
    name: String,
    total_left: i64,
    self_left: i64,
    total_right: i64,
    self_right: i64,
    children: Vec<usize>,
}

#[derive(Clone, Debug)]
struct Bar {
    node: Option<usize>,
    name: String,
    total_left: i64,
    self_left: i64,
    total_right: i64,
    self_right: i64,
    x_left: i64,
    x_right: i64,
}

#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn diff_trees(left: Tree, right: Tree, max_nodes: i64) -> FlameGraphDiff {
    let (left_root, left_nodes) = left.snapshot();
    let (right_root, right_nodes) = right.snapshot();
    let mut merged = Vec::new();
    let root = merge_node(
        Some(left_root),
        &left_nodes,
        Some(right_root),
        &right_nodes,
        ROOT_NAME,
        &mut merged,
    );
    let keep = keep_set(&merged, root, max_nodes);
    let mut names = vec![ROOT_NAME.to_string()];
    let mut name_index = HashMap::from([(ROOT_NAME.to_string(), 0_i64)]);
    let mut levels = Vec::new();
    let mut current = vec![Bar {
        node: Some(root),
        name: ROOT_NAME.to_string(),
        total_left: merged[root].total_left,
        self_left: merged[root].self_left,
        total_right: merged[root].total_right,
        self_right: merged[root].self_right,
        x_left: 0,
        x_right: 0,
    }];

    while !current.is_empty() {
        let mut values = Vec::with_capacity(current.len() * 7);
        let mut next = Vec::new();
        let mut previous_left_end = 0;
        let mut previous_right_end = 0;
        for bar in &current {
            let name_idx = name_slot(&mut names, &mut name_index, &bar.name);
            values.extend([
                bar.x_left - previous_left_end,
                bar.total_left,
                bar.self_left,
                bar.x_right - previous_right_end,
                bar.total_right,
                bar.self_right,
                name_idx,
            ]);
            previous_left_end = bar.x_left + bar.total_left;
            previous_right_end = bar.x_right + bar.total_right;
            append_children(&merged, &keep, bar, &mut next);
        }
        levels.push(Level { values });
        current = next;
    }

    FlameGraphDiff {
        names,
        levels,
        left_ticks: merged[root].total_left,
        right_ticks: merged[root].total_right,
    }
}

fn merge_node(
    left: Option<usize>,
    left_nodes: &[TreeSnapshotNode],
    right: Option<usize>,
    right_nodes: &[TreeSnapshotNode],
    fallback_name: &str,
    out: &mut Vec<MergedNode>,
) -> usize {
    let name = left
        .map(|idx| left_nodes[idx].name.clone())
        .or_else(|| right.map(|idx| right_nodes[idx].name.clone()))
        .unwrap_or_else(|| fallback_name.to_string());
    let idx = out.len();
    out.push(MergedNode {
        name,
        total_left: left.map_or(0, |node| left_nodes[node].total),
        self_left: left.map_or(0, |node| left_nodes[node].self_),
        total_right: right.map_or(0, |node| right_nodes[node].total),
        self_right: right.map_or(0, |node| right_nodes[node].self_),
        children: Vec::new(),
    });

    let left_children = children_by_name(left, left_nodes);
    let right_children = children_by_name(right, right_nodes);
    let child_names: BTreeSet<&String> =
        left_children.keys().chain(right_children.keys()).collect();
    for name in child_names {
        let child = merge_node(
            left_children.get(name).copied(),
            left_nodes,
            right_children.get(name).copied(),
            right_nodes,
            name,
            out,
        );
        out[idx].children.push(child);
    }
    idx
}

fn children_by_name(node: Option<usize>, nodes: &[TreeSnapshotNode]) -> BTreeMap<String, usize> {
    node.map_or_else(BTreeMap::new, |idx| {
        nodes[idx]
            .children
            .iter()
            .map(|child| (nodes[*child].name.clone(), *child))
            .collect()
    })
}

fn keep_set(nodes: &[MergedNode], root: usize, max_nodes: i64) -> HashSet<usize> {
    if max_nodes <= 0 || nodes.len() <= usize::try_from(max_nodes).unwrap_or(usize::MAX) {
        return (0..nodes.len()).collect();
    }
    let max_nodes = usize::try_from(max_nodes).unwrap_or(usize::MAX);
    let parents = parents(nodes);
    let mut ranked: Vec<usize> = (0..nodes.len()).filter(|node| *node != root).collect();
    ranked.sort_by(|left, right| {
        combined_total(nodes, *right)
            .cmp(&combined_total(nodes, *left))
            .then_with(|| nodes[*left].name.cmp(&nodes[*right].name))
            .then_with(|| left.cmp(right))
    });
    let mut keep = HashSet::from([root]);
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

fn parents(nodes: &[MergedNode]) -> Vec<Option<usize>> {
    let mut parents = vec![None; nodes.len()];
    for (parent, node) in nodes.iter().enumerate() {
        for child in &node.children {
            parents[*child] = Some(parent);
        }
    }
    parents
}

fn combined_total(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].total_left + nodes[node].total_right
}

fn append_children(nodes: &[MergedNode], keep: &HashSet<usize>, parent: &Bar, next: &mut Vec<Bar>) {
    let Some(parent_node) = parent.node else {
        return;
    };
    let mut x_left = parent.x_left;
    let mut x_right = parent.x_right;
    let mut other_total_left = 0;
    let mut other_self_left = 0;
    let mut other_total_right = 0;
    let mut other_self_right = 0;
    for child in &nodes[parent_node].children {
        let node = &nodes[*child];
        if keep.contains(child) {
            next.push(Bar {
                node: Some(*child),
                name: node.name.clone(),
                total_left: node.total_left,
                self_left: node.self_left,
                total_right: node.total_right,
                self_right: node.self_right,
                x_left,
                x_right,
            });
        } else {
            other_total_left += node.total_left;
            other_self_left += subtree_self_left(nodes, *child);
            other_total_right += node.total_right;
            other_self_right += subtree_self_right(nodes, *child);
        }
        x_left += node.total_left;
        x_right += node.total_right;
    }
    if other_total_left > 0 || other_total_right > 0 {
        next.push(Bar {
            node: None,
            name: OTHER_NAME.to_string(),
            total_left: other_total_left,
            self_left: other_self_left,
            total_right: other_total_right,
            self_right: other_self_right,
            x_left: parent.x_left + parent.total_left - other_total_left,
            x_right: parent.x_right + parent.total_right - other_total_right,
        });
    }
}

fn subtree_self_left(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_left
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_left(nodes, *child))
            .sum::<i64>()
}

fn subtree_self_right(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_right
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_right(nodes, *child))
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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{Frame, Tree};

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    #[test]
    fn diff_aligns_right_only_frame_with_zero_left() {
        let mut left = Tree::new();
        left.add_stack(&[frame("a")], 10);
        let mut right = Tree::new();
        right.add_stack(&[frame("a")], 10);
        right.add_stack(&[frame("b")], 5);

        let diff = diff_trees(left, right, 0);
        assert!(diff.left_ticks == 10);
        assert!(diff.right_ticks == 15);
        for level in &diff.levels {
            assert!(level.values.len() % 7 == 0);
        }

        let b_idx = i64::try_from(diff.names.iter().position(|name| name == "b").unwrap()).unwrap();
        let level1 = &diff.levels[1].values;
        let b_bar = level1.chunks(7).find(|chunk| chunk[6] == b_idx).unwrap();
        assert!(b_bar[1] == 0);
        assert!(b_bar[2] == 0);
        assert!(b_bar[4] == 5);
        assert!(b_bar[5] == 5);
    }

    #[test]
    fn diff_root_is_total_on_both_sides() {
        let mut left = Tree::new();
        left.add_stack(&[frame("a")], 3);
        let mut right = Tree::new();
        right.add_stack(&[frame("a")], 9);

        let diff = diff_trees(left, right, 0);
        let root = &diff.levels[0].values;
        assert!(root[1] == 3 && root[4] == 9);
        assert!(diff.names[usize::try_from(root[6]).unwrap()] == "total");
    }
}

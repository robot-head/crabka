//! Nested-set interval assignment for trace span forests.

use std::collections::HashMap;

/// One span's tree linkage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanNode {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
}

/// The three nested-set columns for one span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub parent_id: i32,
}

/// Assign nested-set intervals by DFS preorder over the trace forest.
#[must_use]
pub fn assign_nested_set(spans: &[SpanNode]) -> Vec<NestedSet> {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let pos: HashMap<[u8; 8], usize> = spans
        .iter()
        .enumerate()
        .map(|(i, s)| (s.span_id, i))
        .collect();

    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();
    for (i, span) in spans.iter().enumerate() {
        match span.parent_span_id.and_then(|p| pos.get(&p).copied()) {
            Some(parent_idx) if parent_idx != i => children[parent_idx].push(i),
            _ => roots.push(i),
        }
    }

    let mut out = vec![
        NestedSet {
            nested_set_left: 0,
            // -1 is Tempo's no-parent (root) sentinel; left values start at 1 so
            // it never collides with a real parent's left.
            nested_set_right: 0,
            parent_id: -1,
        };
        spans.len()
    ];
    let mut counter = 1_i32;
    let mut visited = vec![false; spans.len()];

    // DFS from the discovered roots. Then sweep for any span the DFS never
    // reached — under cyclic/garbage parentage (e.g. A.parent=B, B.parent=A)
    // a node can be neither a root nor a descendant of one, so it would keep
    // `{0, 0, 0}` and collide with real roots. Seed each such span as an
    // additional root so every node gets a valid `left < right` interval.
    let mut stack = Vec::new();
    for root in roots.iter().copied().chain(0..spans.len()) {
        stack.push(Frame::Enter {
            idx: root,
            // Root span, or cycle-orphaned span re-seeded as a root:
            // nestedSetParent = -1 (Tempo no-parent sentinel).
            parent_left: -1,
        });
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { idx, parent_left } => {
                    if visited[idx] {
                        continue;
                    }
                    visited[idx] = true;
                    let left = counter;
                    counter += 1;
                    out[idx].nested_set_left = left;
                    out[idx].parent_id = parent_left;
                    stack.push(Frame::Exit { idx });
                    for &child in children[idx].iter().rev() {
                        stack.push(Frame::Enter {
                            idx: child,
                            parent_left: left,
                        });
                    }
                }
                Frame::Exit { idx } => {
                    out[idx].nested_set_right = counter;
                    counter += 1;
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    fn sid(n: u8) -> [u8; 8] {
        [n, 0, 0, 0, 0, 0, 0, 0]
    }

    fn node(id: u8, parent: Option<u8>) -> SpanNode {
        SpanNode {
            span_id: sid(id),
            parent_span_id: parent.map(sid),
        }
    }

    fn sample_tree() -> Vec<SpanNode> {
        vec![
            node(1, None),
            node(2, Some(1)),
            node(3, Some(1)),
            node(4, Some(3)),
        ]
    }

    fn idx(spans: &[SpanNode], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn root_has_sentinel_parent_id() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        // -1 = Tempo's no-parent sentinel (so `nestedSetParent < 0` finds roots).
        assert!(ns[idx(&spans, 1)].parent_id == -1);
    }

    #[test]
    fn child_parent_id_equals_parent_left() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let p_left = ns[idx(&spans, 3)].nested_set_left;
        assert!(ns[idx(&spans, 4)].parent_id == p_left);
        let root_left = ns[idx(&spans, 1)].nested_set_left;
        assert!(ns[idx(&spans, 2)].parent_id == root_left);
        assert!(ns[idx(&spans, 3)].parent_id == root_left);
    }

    #[test]
    fn ancestor_interval_strictly_contains_descendants() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let r = ns[idx(&spans, 1)];
        for id in [2_u8, 3, 4] {
            let d = ns[idx(&spans, id)];
            assert!(r.nested_set_left < d.nested_set_left);
            assert!(d.nested_set_right < r.nested_set_right);
        }

        let three = ns[idx(&spans, 3)];
        let two = ns[idx(&spans, 2)];
        let four = ns[idx(&spans, 4)];
        assert!(
            three.nested_set_left < four.nested_set_left
                && four.nested_set_right < three.nested_set_right
        );
        assert!(
            !(two.nested_set_left < four.nested_set_left
                && four.nested_set_right < two.nested_set_right)
        );
    }

    #[test]
    fn orphan_is_treated_as_root() {
        let spans = vec![node(5, Some(99))];
        let ns = assign_nested_set(&spans);
        assert!(ns[0].parent_id == -1); // dangling parent → root sentinel
        assert!(ns[0].nested_set_left < ns[0].nested_set_right);
    }

    #[test]
    fn left_lt_right_for_every_node() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        for n in &ns {
            assert!(n.nested_set_left < n.nested_set_right);
        }
    }

    #[test]
    fn cyclic_parentage_still_assigns_valid_intervals() {
        // A.parent = B, B.parent = A. Neither is a root, so a DFS seeded only
        // from roots would never visit them and leave both at {0, 0, 0} —
        // colliding with real roots and corrupting structural ops. Every node
        // must still get a valid `left < right` interval.
        let spans = vec![node(1, Some(2)), node(2, Some(1))];
        let ns = assign_nested_set(&spans);
        for n in &ns {
            assert!(n.nested_set_left < n.nested_set_right);
        }
        // Distinct, non-overlapping intervals → every node was actually visited.
        let mut lefts: Vec<i32> = ns.iter().map(|n| n.nested_set_left).collect();
        lefts.sort_unstable();
        lefts.dedup();
        assert!(lefts.len() == ns.len());
        assert!(ns.iter().any(|n| n.parent_id == -1));
    }

    #[test]
    fn three_node_cycle_is_fully_visited() {
        // A->B->C->A: a pure cycle with no acyclic entry point.
        let spans = vec![node(1, Some(3)), node(2, Some(1)), node(3, Some(2))];
        let ns = assign_nested_set(&spans);
        for n in &ns {
            assert!(n.nested_set_left < n.nested_set_right);
        }
        let mut lefts: Vec<i32> = ns.iter().map(|n| n.nested_set_left).collect();
        lefts.sort_unstable();
        lefts.dedup();
        assert!(lefts.len() == ns.len());
        assert!(ns.iter().any(|n| n.parent_id == -1));
    }

    #[test]
    fn cyclic_parentage_assignment_completes() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let spans = vec![node(1, None), node(2, Some(3)), node(3, Some(2))];
            let _ = tx.send(assign_nested_set(&spans));
        });

        let ns = rx
            .recv_timeout(Duration::from_millis(250))
            .expect("cyclic parentage assignment should complete");
        assert!(ns.len() == 3);
        assert!(ns.iter().all(|node| node.nested_set_left > 0));
    }
}

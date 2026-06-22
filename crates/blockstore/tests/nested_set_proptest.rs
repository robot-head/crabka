//! Random-forest properties for nested-set interval assignment.

use std::collections::HashMap;

use crabka_blockstore::{SpanNode, assign_nested_set};
use proptest::prelude::*;

fn sid(n: u32) -> [u8; 8] {
    let b = n.to_le_bytes();
    [b[0], b[1], b[2], b[3], 0, 0, 0, 0]
}

fn arb_forest() -> impl Strategy<Value = Vec<SpanNode>> {
    (1_usize..24)
        .prop_flat_map(|n| {
            let parents = (1..n)
                .map(|i| prop_oneof![Just(None), (0_usize..i).prop_map(Some)])
                .collect::<Vec<_>>();
            (Just(n), parents)
        })
        .prop_map(|(n, parents)| {
            let mut spans = vec![SpanNode {
                span_id: sid(0),
                parent_span_id: None,
            }];
            for (i, parent) in parents.into_iter().enumerate() {
                let child = u32::try_from(i + 1).unwrap();
                spans.push(SpanNode {
                    span_id: sid(child),
                    parent_span_id: parent.map(|p| sid(u32::try_from(p).unwrap())),
                });
            }
            debug_assert_eq!(spans.len(), n);
            spans
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn nested_set_intervals_are_valid(spans in arb_forest()) {
        let ns = assign_nested_set(&spans);
        let by_id: HashMap<[u8; 8], usize> =
            spans.iter().enumerate().map(|(i, s)| (s.span_id, i)).collect();

        for n in &ns {
            prop_assert!(n.nested_set_left < n.nested_set_right);
        }

        for (i, span) in spans.iter().enumerate() {
            if let Some(parent) = span.parent_span_id.and_then(|p| by_id.get(&p).copied()) {
                if parent == i {
                    prop_assert_eq!(ns[i].parent_id, 0);
                } else {
                    prop_assert_eq!(ns[i].parent_id, ns[parent].nested_set_left);
                    let mut cur = Some(parent);
                    while let Some(ancestor) = cur {
                        prop_assert!(ns[ancestor].nested_set_left < ns[i].nested_set_left);
                        prop_assert!(ns[i].nested_set_right < ns[ancestor].nested_set_right);
                        cur = spans[ancestor]
                            .parent_span_id
                            .and_then(|p| by_id.get(&p).copied());
                    }
                }
            } else {
                prop_assert_eq!(ns[i].parent_id, 0);
            }
        }
    }
}

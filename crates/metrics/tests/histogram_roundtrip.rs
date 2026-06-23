use crabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms,
};
use proptest::prelude::*;

fn arb_span_and_counts() -> impl Strategy<Value = (Vec<BucketSpan>, Vec<f64>)> {
    proptest::collection::vec((0_i32..8, 1_u32..4), 0..3).prop_map(|spans| {
        let spans: Vec<BucketSpan> = spans
            .into_iter()
            .map(|(offset, length)| BucketSpan { offset, length })
            .collect();
        let total: usize = spans.iter().map(|s| s.length as usize).sum();
        let mut next_count = 1.0;
        let counts = (0..total)
            .map(|_| {
                let count = next_count;
                next_count += 1.0;
                count
            })
            .collect();
        (spans, counts)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn random_native_histograms_round_trip(
        schema in -53_i8..=8,
        is_float in any::<bool>(),
        sum in -1e6_f64..1e6,
        (pos_spans, positive_counts) in arb_span_and_counts(),
        (neg_spans, negative_counts) in arb_span_and_counts(),
    ) {
        let h = NativeHistogram {
            schema,
            is_float,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-100,
            zero_count: 0.0,
            count: positive_counts.iter().chain(&negative_counts).sum(),
            sum,
            positive_spans: pos_spans,
            positive_counts,
            negative_spans: neg_spans,
            negative_counts,
            custom_values: None,
            start_timestamp_ms: None,
        };
        let rows = vec![(7_u64, 99_i64, h.clone())];
        let batch = encode_native_histograms(&rows).unwrap();
        let back = decode_native_histograms(&batch).unwrap();
        prop_assert_eq!(back, rows);
    }
}

use crabka_compression::{CompressionType, compress, decompress};
use proptest::prelude::*;

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    // Sizes 0..=32 KiB. Mix of all-zeros (highly compressible) and random
    // (worst case) via prop_oneof.
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..=32 * 1024),
        proptest::collection::vec(0u8..=0u8, 0..=32 * 1024),
    ]
}

macro_rules! roundtrip_for {
    ($name:ident, $ct:expr) => {
        proptest! {
            #[test]
            fn $name(data in arb_payload()) {
                let z = compress($ct, &data).unwrap();
                let back = decompress($ct, &z, usize::MAX).unwrap();
                prop_assert_eq!(back.as_ref(), data.as_slice());
            }
        }
    };
}

roundtrip_for!(none_roundtrip, CompressionType::None);
roundtrip_for!(gzip_roundtrip, CompressionType::Gzip);
roundtrip_for!(snappy_roundtrip, CompressionType::Snappy);
roundtrip_for!(lz4_roundtrip, CompressionType::Lz4);
roundtrip_for!(zstd_roundtrip, CompressionType::Zstd);

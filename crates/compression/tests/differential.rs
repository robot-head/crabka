mod support;
use crabka_compression::{CompressionType, compress, decompress};
use proptest::prelude::*;
use support::oracle;

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..=8 * 1024),
        proptest::collection::vec(0u8..=0u8, 0..=8 * 1024),
    ]
}

macro_rules! diff_test {
    ($name:ident, $codec_str:literal, $ct:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            // proptest! requires an `Fn` closure; wrap the guard in RefCell
            // so we can call `&mut Oracle` methods via interior mutability.
            let o = std::cell::RefCell::new(oracle::shared());
            proptest!(|(data in arb_payload())| {
                // Rust compresses; JVM decompresses; bytes match input.
                let rust_z = compress($ct, &data).unwrap();
                let jvm_back = o.borrow_mut().decompress($codec_str, &rust_z);
                prop_assert_eq!(jvm_back, data.clone());

                // JVM compresses; Rust decompresses; bytes match input.
                let jvm_z = o.borrow_mut().compress($codec_str, &data);
                let rust_back = decompress($ct, &jvm_z, usize::MAX).unwrap();
                prop_assert_eq!(rust_back.as_ref(), data.as_slice());
            });
        }
    };
}

diff_test!(gzip_differential, "gzip", CompressionType::Gzip);
diff_test!(snappy_differential, "snappy", CompressionType::Snappy);
diff_test!(lz4_differential, "lz4", CompressionType::Lz4);
diff_test!(zstd_differential, "zstd", CompressionType::Zstd);

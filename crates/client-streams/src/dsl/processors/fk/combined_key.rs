//! `CombinedKey<KO,K>` byte codec (JVM `CombinedKeySchema`).
//! Layout: `[ foreignKeyLen : 4 bytes BE ] [ foreignKeyBytes ] [ primaryKeyBytes ]`.
//! Consumed by the FK-join subscription store (`store::fk_subscription`).
use bytes::{BufMut, Bytes, BytesMut};

#[must_use]
pub(crate) fn combined_key(fk: &[u8], pk: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + fk.len() + pk.len());
    b.put_u32(u32::try_from(fk.len()).expect("fk len fits u32"));
    b.extend_from_slice(fk);
    b.extend_from_slice(pk);
    b.freeze()
}

#[must_use]
pub(crate) fn foreign_prefix(fk: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + fk.len());
    b.put_u32(u32::try_from(fk.len()).expect("fk len fits u32"));
    b.extend_from_slice(fk);
    b.freeze()
}

#[must_use]
pub(crate) fn split_combined_key(k: &[u8]) -> (&[u8], &[u8]) {
    let fk_len = u32::from_be_bytes(k[..4].try_into().expect("4 bytes")) as usize;
    (&k[4..4 + fk_len], &k[4 + fk_len..])
}

/// Half-open upper bound covering every combined key with foreign prefix `fk`:
/// the lexicographic successor of `foreign_prefix(fk)` (strip trailing 0xFF, +1).
#[must_use]
pub(crate) fn range_upper(fk: &[u8]) -> Bytes {
    let mut p = foreign_prefix(fk).to_vec();
    while let Some(last) = p.last().copied() {
        if last == 0xFF {
            p.pop();
        } else {
            *p.last_mut().unwrap() = last + 1;
            break;
        }
    }
    Bytes::from(p)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn round_trip_and_prefix() {
        let fk = b"foreign";
        let pk = b"primary";
        let k = combined_key(fk, pk);
        assert_eq!(&k[..4], &u32::try_from(fk.len()).unwrap().to_be_bytes());
        assert_eq!(&k[4..4 + fk.len()], fk);
        assert_eq!(&k[4 + fk.len()..], pk);
        assert_eq!(foreign_prefix(fk).as_ref(), &k[..4 + fk.len()]);
        let (gfk, gpk) = split_combined_key(&k);
        assert_eq!(gfk, fk);
        assert_eq!(gpk, pk);
    }

    #[test]
    fn matches_jvm_capture() {
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/testdata/fk_join/behavior.json"
            ))
            .unwrap(),
        )
        .unwrap();
        for e in v["combined_key"].as_array().unwrap() {
            let fk = e["fk"].as_str().unwrap().as_bytes();
            let pk = e["pk"].as_str().unwrap().as_bytes();
            assert_eq!(
                combined_key(fk, pk),
                Bytes::from(hex(e["bytes_hex"].as_str().unwrap()))
            );
            assert_eq!(
                foreign_prefix(fk),
                Bytes::from(hex(e["prefix_hex"].as_str().unwrap()))
            );
        }
    }
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}

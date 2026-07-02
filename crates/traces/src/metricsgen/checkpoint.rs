//! Checkpoint codecs for rebuildable metrics-generator state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, thiserror::Error)]
pub enum CheckpointCodecError {
    #[error("truncated checkpoint key")]
    Truncated,
    #[error("invalid utf8 in checkpoint tenant")]
    Utf8,
    #[error("bad trace id length in checkpoint key")]
    BadTraceId,
    #[error("bad edge id length in checkpoint key")]
    BadEdgeId,
    #[error("bad connection type in checkpoint value")]
    BadConnectionType,
}

#[must_use]
pub fn encode_checkpoint_key(tenant: &str, trace_id: &[u8; 16], edge_id: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    put_bytes(&mut buf, tenant.as_bytes());
    put_bytes(&mut buf, trace_id);
    put_bytes(&mut buf, edge_id);
    buf.freeze()
}

pub fn parse_checkpoint_key(
    mut buf: &[u8],
) -> Result<(String, [u8; 16], Vec<u8>), CheckpointCodecError> {
    let tenant = String::from_utf8(get_bytes(&mut buf)?).map_err(|_| CheckpointCodecError::Utf8)?;
    let trace_id: [u8; 16] = get_bytes(&mut buf)?
        .try_into()
        .map_err(|_| CheckpointCodecError::BadTraceId)?;
    let edge_id = get_bytes(&mut buf)?;
    Ok((tenant, trace_id, edge_id))
}

fn put_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("checkpoint key segment too long");
    buf.put_u32(len);
    buf.put_slice(bytes);
}

fn get_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, CheckpointCodecError> {
    if buf.len() < 4 {
        return Err(CheckpointCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(CheckpointCodecError::Truncated);
    }
    let bytes = buf[..len].to_vec();
    buf.advance(len);
    Ok(bytes)
}

pub trait EdgeCheckpointStore: Send + Sync {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]);
    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn tenants(&self) -> Vec<String>;
}

type StoreKey = (String, Vec<u8>);

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    inner: Arc<Mutex<BTreeMap<StoreKey, Vec<u8>>>>,
}

impl EdgeCheckpointStore for InMemoryCheckpointStore {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]) {
        let mut inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let store_key = (tenant.to_string(), key.to_vec());
        if value.is_empty() {
            inner.remove(&store_key);
        } else {
            inner.insert(store_key, value.to_vec());
        }
    }

    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        inner
            .iter()
            .filter(|((stored_tenant, _), _)| stored_tenant == tenant)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }

    fn tenants(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let mut tenants: Vec<_> = inner
            .keys()
            .map(|(tenant, _)| tenant.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        tenants.sort();
        tenants
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn checkpoint_key_round_trips() {
        let trace = [0x22; 16];
        let key = encode_checkpoint_key("tenant-a", &trace, &[0xAA, 0xBB]);
        let parsed = parse_checkpoint_key(&key).unwrap();

        assert_eq!(parsed, ("tenant-a".to_string(), trace, vec![0xAA, 0xBB]));
    }

    #[test]
    fn checkpoint_key_rejects_truncated_bytes() {
        let trace = [0x22; 16];
        let key = encode_checkpoint_key("tenant-a", &trace, &[0xAA, 0xBB]);
        let truncated = &key[..key.len() - 1];

        assert!(matches!(
            parse_checkpoint_key(truncated),
            Err(CheckpointCodecError::Truncated)
        ));
    }

    #[test]
    fn in_memory_store_round_trips_tombstones_and_isolates_tenants() {
        let store = InMemoryCheckpointStore::default();
        store.save("t", b"k1", b"v1");
        store.save("t", b"k2", b"v2");

        let all = store.load_all("t");
        assert!(all.len() == 2);

        store.save("t", b"k1", b"");
        let after_tombstone = store.load_all("t");
        assert_eq!(after_tombstone, vec![(b"k2".to_vec(), b"v2".to_vec())]);
        check!(store.load_all("other").is_empty());
        check!(store.tenants() == vec!["t".to_string()]);
    }
}

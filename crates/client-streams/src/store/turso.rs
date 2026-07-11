//! Turso-backed `ByteKeyValueStore`. One table `kv(k BLOB PRIMARY KEY, v BLOB)`
//! per store; UPSERT on put; half-open ordered range scan. Async-native (no `block_on`).
use async_trait::async_trait;
use bytes::Bytes;
use turso::Connection;

use crate::store::byte::ByteKeyValueStore;

// turso 0.6 `Value` has no `TryInto<Vec<u8>>`: extract a BLOB by matching.
fn blob(v: turso::Value) -> Vec<u8> {
    match v {
        turso::Value::Blob(b) => b,
        other => panic!("expected BLOB column, got {other:?}"),
    }
}

// `COUNT(*)` and other integer columns arrive as `Value::Integer`.
fn integer(v: turso::Value) -> i64 {
    match v {
        turso::Value::Integer(n) => n,
        other => panic!("expected INTEGER column, got {other:?}"),
    }
}

pub(crate) struct TursoBytes {
    conn: Connection,
}

impl TursoBytes {
    /// Open (or create) the store DB at `path`. Clean-slate: the changelog is the
    /// source of truth, so restore replays into an empty table.
    pub(crate) async fn open(path: &str) -> Self {
        let db = turso::Builder::new_local(path)
            .build()
            .await
            .expect("open turso db");
        let conn = db.connect().expect("turso connect");
        conn.execute("DROP TABLE IF EXISTS kv", ())
            .await
            .expect("drop kv");
        conn.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)", ())
            .await
            .expect("create kv");
        Self { conn }
    }

    #[cfg(test)]
    pub(crate) async fn open_in_memory() -> Self {
        Self::open(":memory:").await
    }
}

#[async_trait]
impl ByteKeyValueStore for TursoBytes {
    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mut rows = self
            .conn
            .query("SELECT v FROM kv WHERE k = ?1", (key,))
            .await
            .expect("turso get");
        let row = rows.next().await.expect("turso row")?;
        Some(Bytes::from(blob(row.get_value(0).expect("turso v"))))
    }

    async fn put(&mut self, key: Bytes, value: Bytes) {
        self.conn
            .execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2) \
                 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (key.as_ref(), value.as_ref()),
            )
            .await
            .expect("turso put");
    }

    async fn delete(&mut self, key: &[u8]) -> Option<Bytes> {
        let prev = self.get(key).await;
        self.conn
            .execute("DELETE FROM kv WHERE k = ?1", (key,))
            .await
            .expect("turso delete");
        prev
    }

    async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut rows = self
            .conn
            .query(
                "SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k",
                (lo, hi),
            )
            .await
            .expect("turso range");
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.expect("turso range row") {
            let k = blob(row.get_value(0).expect("k"));
            let v = blob(row.get_value(1).expect("v"));
            out.push((Bytes::from(k), Bytes::from(v)));
        }
        out
    }

    async fn scan_all(&self) -> Vec<(Bytes, Bytes)> {
        let mut rows = self
            .conn
            .query("SELECT k, v FROM kv ORDER BY k", ())
            .await
            .expect("turso scan_all");
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.expect("turso scan_all row") {
            let k = blob(row.get_value(0).expect("k"));
            let v = blob(row.get_value(1).expect("v"));
            out.push((Bytes::from(k), Bytes::from(v)));
        }
        out
    }

    async fn approx_len(&self) -> u64 {
        let mut rows = self
            .conn
            .query("SELECT COUNT(*) FROM kv", ())
            .await
            .expect("turso approx_len");
        let row = rows
            .next()
            .await
            .expect("turso approx_len row")
            .expect("count row present");
        // `COUNT(*)` is non-negative; the fallback never triggers.
        u64::try_from(integer(row.get_value(0).expect("count"))).unwrap_or(0)
    }

    async fn clear(&mut self) {
        self.conn
            .execute("DELETE FROM kv", ())
            .await
            .expect("turso clear");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};

    /// Both backends must satisfy the same byte-KV contract.
    async fn contract(mut s: Box<dyn ByteKeyValueStore>) {
        assert_eq!(s.get(b"k").await, None);
        s.put(Bytes::from_static(b"k"), Bytes::from_static(b"v1"))
            .await;
        s.put(Bytes::from_static(b"k"), Bytes::from_static(b"v2"))
            .await; // upsert
        assert_eq!(s.get(b"k").await, Some(Bytes::from_static(b"v2")));
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"))
            .await;
        s.put(Bytes::from_static(&[1, 9]), Bytes::from_static(b"b"))
            .await;
        let r = s.range(&[1, 0], &[1, 5]).await; // half-open → only [1,0]
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].1, Bytes::from_static(b"a"));
        assert_eq!(s.delete(b"k").await, Some(Bytes::from_static(b"v2")));
        assert_eq!(s.get(b"k").await, None);
    }

    #[tokio::test]
    async fn inmemory_contract() {
        contract(Box::new(InMemoryBytes::default())).await;
    }

    #[tokio::test]
    async fn turso_contract() {
        contract(Box::new(TursoBytes::open_in_memory().await)).await;
    }
}

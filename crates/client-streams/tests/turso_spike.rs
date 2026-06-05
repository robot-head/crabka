//! THROWAWAY spike (deleted in a later task): proves turso 0.6 fits our constraints —
//! Connection: Send, futures resolve under tokio .await, ordered BLOB range scan.
use turso::{Builder, Value};

fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn turso_send_await_and_ordered_range() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();
    assert_send(&conn); // (2) Connection: Send

    conn.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)", ())
        .await
        .unwrap();
    for (k, v) in [
        (&[1u8, 0][..], b"a"),
        (&[1u8, 2][..], b"b"),
        (&[2u8, 0][..], b"c"),
    ] {
        conn.execute(
            "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (k, &v[..]),
        )
        .await
        .unwrap();
    }
    let mut rows = conn
        .query(
            "SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k",
            (&[1u8, 0][..], &[2u8, 0][..]),
        )
        .await
        .unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let v = match row.get_value(1).unwrap() {
            Value::Blob(b) => b,
            other => panic!("expected Blob, got {other:?}"),
        };
        got.push(v);
    }
    // (4) ordered, half-open [0x0100, 0x0200) — should contain rows a and b, not c
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec()]);
}

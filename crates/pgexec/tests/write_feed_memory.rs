use crabka_pgexec::{RuntimePolicy, SqlEngine};
use crabka_pgwire::engine::{Engine, Session};

#[tokio::test]
async fn write_feed_uses_configured_blocking_memory() {
    let engine = SqlEngine::new_with_policy(RuntimePolicy {
        blocking_query_memory: crabka_units::bytes(512),
        ..RuntimePolicy::default()
    })
    .expect("runtime policy");
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE source (v text)")
        .await
        .expect("source table");
    let payload = "x".repeat(100);
    let values = std::iter::repeat_n(format!("('{payload}')"), 10)
        .collect::<Vec<_>>()
        .join(",");
    session
        .simple_query(&format!("INSERT INTO source VALUES {values}"))
        .await
        .expect("source rows");

    let error = session
        .simple_query("CREATE TABLE destination AS SELECT * FROM source")
        .await
        .expect_err("configured write-feed budget must apply");
    assert_eq!(error.code, "53200", "{error:?}");
}

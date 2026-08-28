use assert2::assert;
use crabka_pgexec::{RuntimePolicy, SqlEngine};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

fn constrained_engine() -> SqlEngine {
    SqlEngine::new_with_policy(RuntimePolicy {
        blocking_query_memory: crabka_units::bytes(1),
        ..Default::default()
    })
    .expect("policy")
}

#[tokio::test]
async fn runtime_budget_reaches_set_operations_and_recursive_ctes() {
    for sql in [
        "SELECT 1 UNION SELECT 2",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 2) \
         SELECT n FROM t",
    ] {
        let error = constrained_engine()
            .connect()
            .simple_query(sql)
            .await
            .expect_err("materialization must use the runtime limit");
        assert!(error.code == "53200", "{sql}: {error:?}");
    }
}

#[tokio::test]
async fn work_mem_does_not_replace_the_runtime_statement_cap() {
    let mut session = SqlEngine::new().connect();
    let sql = "SELECT i FROM generate_series(1, 1000) AS g(i) \
               UNION SELECT i FROM generate_series(1, 1000) AS g(i)";
    session
        .simple_query(sql)
        .await
        .expect("runtime policy fits");
    session
        .simple_query("SET work_mem = '64kB'")
        .await
        .expect("set work_mem");
    session
        .simple_query(sql)
        .await
        .expect("work_mem does not replace the runtime statement cap");
}

#[tokio::test]
async fn scalar_udf_and_builtin_srf_share_a_select_list() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query(
            "CREATE FUNCTION scalar_plus_one(n int) RETURNS int LANGUAGE sql AS 'SELECT n + 1'",
        )
        .await
        .expect("create function");

    let result = session
        .simple_query("SELECT scalar_plus_one(1), generate_series(1, 2)")
        .await
        .expect("query");
    let QueryResult::Rows { rows, .. } = &result[0] else {
        panic!("expected rows");
    };
    let values: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    String::from_utf8(cell.as_ref().expect("non-null").text.to_vec())
                        .expect("utf-8")
                })
                .collect()
        })
        .collect();

    assert!(
        values
            == vec![
                vec![String::from("2"), String::from("1")],
                vec![String::from("2"), String::from("2")],
            ]
    );
}

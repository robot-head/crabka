use std::sync::atomic::{AtomicU64, Ordering};

use crabka_gres_conformance::{
    ExtendedCase, ExtendedParam, ExtendedParamType, ExtendedParamValue, run_extended_one, run_one,
};
use tokio_postgres::{Client, NoTls};

static NEXT_TABLE: AtomicU64 = AtomicU64::new(0);

fn live_url() -> Option<String> {
    std::env::var("CRABKA_GRES_PGDOG_TEST_URL").ok()
}

fn unique_table(prefix: &str) -> String {
    format!(
        "gres_ext_test_{prefix}_{}_{}",
        std::process::id(),
        NEXT_TABLE.fetch_add(1, Ordering::Relaxed)
    )
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect to PgDog test endpoint");
    tokio::spawn(async move {
        connection.await.expect("drive PgDog test connection");
    });
    client
}

fn select_case(table: &str, setup: Vec<String>, teardown: Vec<String>) -> ExtendedCase {
    ExtendedCase {
        name: "transaction_scoped_row".into(),
        sql: format!("SELECT label FROM {table} WHERE id = $1"),
        params: vec![ExtendedParam {
            ty: ExtendedParamType::Int4,
            value: Some(ExtendedParamValue::Int4(7)),
        }],
        setup,
        teardown,
    }
}

#[tokio::test]
async fn setup_and_prepared_query_share_a_rolled_back_transaction_through_pgdog() {
    let Some(url) = live_url() else {
        return;
    };
    let table = unique_table("rollback");
    let mut client = connect(&url).await;
    client
        .batch_execute(&format!("CREATE TABLE {table} (id int4, label text)"))
        .await
        .expect("create transaction probe table");
    let case = select_case(
        &table,
        vec![format!("INSERT INTO {table} VALUES (7, 'pinned')")],
        Vec::new(),
    );

    let outcome = run_extended_one(&mut client, &case).await;

    assert_eq!(outcome.error_code, None);
    assert_eq!(outcome.rows, vec![vec![Some("pinned".into())]]);
    let after = run_one(&client, &format!("SELECT * FROM {table}")).await;
    assert_eq!(after.rows, Vec::<Vec<Option<String>>>::new());
    client
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .expect("drop transaction probe table");
}

#[tokio::test]
async fn failures_cleanup_preserve_primary_error_and_allow_rerun() {
    let Some(url) = live_url() else {
        return;
    };
    let table = unique_table("recovery");
    let mut client = connect(&url).await;
    let setup_failure = select_case(
        &table,
        vec![
            format!("CREATE TABLE {table} (id int4, label text)"),
            "SELECT * FROM definitely_missing_setup_table".into(),
        ],
        vec![format!("DROP TABLE {table}")],
    );
    assert_eq!(
        run_extended_one(&mut client, &setup_failure)
            .await
            .error_code
            .as_deref(),
        Some("42P01")
    );
    assert_eq!(
        run_one(&client, &format!("SELECT * FROM {table}"))
            .await
            .error_code
            .as_deref(),
        Some("42P01")
    );

    let parameter_failure = ExtendedCase {
        params: vec![ExtendedParam {
            ty: ExtendedParamType::Bool,
            value: Some(ExtendedParamValue::Int4(7)),
        }],
        setup: vec![format!("CREATE TABLE {table} (id int4, label text)")],
        teardown: vec![format!("DROP TABLE {table}")],
        ..setup_failure.clone()
    };
    assert!(
        run_extended_one(&mut client, &parameter_failure)
            .await
            .error_code
            .is_some_and(|code| code.starts_with("XXPARAM:"))
    );
    assert_eq!(
        run_one(&client, &format!("SELECT * FROM {table}"))
            .await
            .error_code
            .as_deref(),
        Some("42P01")
    );

    let query_failure = ExtendedCase {
        sql: "SELECT * FROM definitely_missing_query_table WHERE id = $1".into(),
        teardown: vec![
            format!("DROP TABLE {table}"),
            "DROP TABLE definitely_missing_cleanup_table".into(),
        ],
        setup: vec![format!("CREATE TABLE {table} (id int4, label text)")],
        ..setup_failure.clone()
    };
    assert_eq!(
        run_extended_one(&mut client, &query_failure)
            .await
            .error_code
            .as_deref(),
        Some("42P01"),
        "query error remains primary when cleanup also fails"
    );

    let success = select_case(
        &table,
        vec![
            format!("CREATE TABLE {table} (id int4, label text)"),
            format!("INSERT INTO {table} VALUES (7, 'recovered')"),
        ],
        vec![format!("DROP TABLE {table}")],
    );
    assert_eq!(
        run_extended_one(&mut client, &success).await.error_code,
        None
    );
}

#[tokio::test]
async fn cleanup_error_is_visible_after_success_and_concurrent_cases_do_not_collide() {
    let Some(url) = live_url() else {
        return;
    };
    let table = unique_table("cleanup_error");
    let mut client = connect(&url).await;
    let cleanup_failure = select_case(
        &table,
        vec![
            format!("CREATE TABLE {table} (id int4, label text)"),
            format!("INSERT INTO {table} VALUES (7, 'ok')"),
        ],
        vec!["DROP TABLE definitely_missing_cleanup_table".into()],
    );
    assert_eq!(
        run_extended_one(&mut client, &cleanup_failure)
            .await
            .error_code
            .as_deref(),
        Some("42P01")
    );
    client
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .expect("remove table left by intentional cleanup failure");

    let mut tasks = Vec::new();
    for _ in 0..2 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let table = unique_table("concurrent");
            let mut client = connect(&url).await;
            let case = select_case(
                &table,
                vec![
                    format!("CREATE TABLE {table} (id int4, label text)"),
                    format!("INSERT INTO {table} VALUES (7, 'parallel')"),
                ],
                vec![format!("DROP TABLE {table}")],
            );
            run_extended_one(&mut client, &case).await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.expect("join concurrent case").error_code, None);
    }
}

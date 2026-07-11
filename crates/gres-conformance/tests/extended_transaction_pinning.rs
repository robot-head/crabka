use crabka_gres_conformance::{
    ExtendedCase, ExtendedParam, ExtendedParamType, ExtendedParamValue, run_extended_one, run_one,
};

#[tokio::test]
async fn extended_case_is_pinned_and_rolled_back_through_transaction_pooler() {
    let Ok(url) = std::env::var("CRABKA_GRES_PGDOG_TEST_URL") else {
        eprintln!("skipping: CRABKA_GRES_PGDOG_TEST_URL is not set");
        return;
    };
    let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to PgDog test endpoint");
    let connection_task = tokio::spawn(async move {
        connection.await.expect("drive PgDog test connection");
    });
    let case = ExtendedCase {
        name: "transaction_pooler_temp_table".into(),
        sql: "SELECT label FROM gres_ext_where WHERE id = $1".into(),
        params: vec![ExtendedParam {
            ty: ExtendedParamType::Int4,
            value: Some(ExtendedParamValue::Int4(7)),
        }],
        setup: vec![
            "CREATE TABLE gres_ext_where (id int4, label text)".into(),
            "INSERT INTO gres_ext_where VALUES (7, 'pinned')".into(),
        ],
        teardown: vec!["DROP TABLE gres_ext_where".into()],
    };

    let outcome = run_extended_one(&mut client, &case).await;

    assert_eq!(outcome.error_code, None);
    assert_eq!(outcome.rows, vec![vec![Some("pinned".into())]]);
    let after = run_one(&client, "SELECT * FROM gres_ext_where").await;
    assert_eq!(after.error_code.as_deref(), Some("42P01"));

    drop(client);
    connection_task.await.expect("join PgDog connection task");
}

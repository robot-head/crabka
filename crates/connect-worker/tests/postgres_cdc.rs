#![cfg(unix)]

//! Docker-backed acceptance proof for the managed Postgres CDC worker.

use std::{io, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use crabka_connect::{ConnectorHandle, ConnectorRuntime, RuntimeState, SecretString};
use crabka_connect_postgres::{
    ColumnValue, EntityKey, PostgresSourceConfig, PostgresWalSource, model::ScalarValue,
    schema::PostgresProtoEncoder,
};
use crabka_connect_worker::{KafkaCheckpointStore, KafkaSink};
use crabka_units::millis;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort as _, WaitFor},
    runners::AsyncRunner as _,
};
use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{Client, NoTls};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const POSTGRES_PORT: u16 = 5432;
const TOPIC: &str = "db.public.orders";
const CONNECTOR_ID: &str = "orders-cdc-acceptance";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    operation: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn postgres_cdc_survives_checkpointed_worker_restart() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();
    let postgres = start_postgres().await?;
    let postgres_port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/app");
    let (database, database_connection) = connect_postgres(&database_url).await?;
    database
        .batch_execute("CREATE TABLE public.orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)")
        .await?;

    let log_dir = tempfile::TempDir::new()?;
    let broker_addr = reserve_address()?;
    let mut broker_config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    broker_config.listen_addr = broker_addr;
    broker_config.advertised_listener = broker_addr.to_string();
    let broker = Broker::start(broker_config).await?;
    let bootstrap = broker.listen_addr().to_string();

    let first_runtime = start_connector(&database_url, &bootstrap).await?;
    wait_for_running(&first_runtime).await?;
    database
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status) VALUES (1, 'pending');
             UPDATE public.orders SET status = 'paid' WHERE id = 1;
             DELETE FROM public.orders WHERE id = 1;
             COMMIT;",
        )
        .await?;

    if let Err(wait_error) = wait_for_record_count(&bootstrap, 3).await {
        let runtime_error = first_runtime.shutdown().await.err();
        return Err(
            io::Error::other(format!("{wait_error}; connector error: {runtime_error:?}")).into(),
        );
    }
    let first = read_records(&bootstrap, "orders-cdc-first", 3).await?;
    let key_one = encoded_key(1)?;
    assert!(first.len() == 3);
    assert!(
        first
            .iter()
            .map(|record| record.operation.as_str())
            .collect::<Vec<_>>()
            == ["insert", "update", "delete"]
    );
    assert!(
        first
            .iter()
            .all(|record| record.key.as_deref() == Some(key_one.as_ref()))
    );
    assert!(first[0].value.is_some());
    assert!(first[1].value.is_some());
    assert!(first[0].value != first[1].value);
    assert!(first[2].value.is_none());

    first_runtime.shutdown().await?;

    let second_runtime = start_connector(&database_url, &bootstrap).await?;
    wait_for_running(&second_runtime).await?;
    database
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (2, 'new')",
            &[],
        )
        .await?;

    wait_for_record_count(&bootstrap, 4).await?;
    let final_records = read_records(&bootstrap, "orders-cdc-final", 4).await?;
    assert!(final_records.len() == 4);
    assert!(final_records[..3] == first);
    assert!(final_records[3].operation == "insert");
    assert!(final_records[3].key.as_deref() == Some(encoded_key(2)?.as_ref()));
    assert!(final_records[3].value.is_some());

    second_runtime.shutdown().await?;
    broker.shutdown().await;
    drop(database);
    database_connection.await??;
    drop(postgres);
    Ok(())
}

fn reserve_address() -> io::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

async fn start_postgres() -> TestResult<ContainerAsync<GenericImage>> {
    Ok(GenericImage::new("postgres", "18")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "app")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await?)
}

async fn connect_postgres(
    database_url: &str,
) -> TestResult<(Client, JoinHandle<Result<(), tokio_postgres::Error>>)> {
    let connected = timeout(WAIT, async {
        loop {
            match tokio_postgres::connect(database_url, NoTls).await {
                Ok((client, connection)) => {
                    return (client, tokio::spawn(connection));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Postgres did not become ready"))?;
    Ok(connected)
}

async fn start_connector(database_url: &str, bootstrap: &str) -> TestResult<ConnectorHandle> {
    let source = PostgresWalSource::connect(PostgresSourceConfig {
        database_url: SecretString::new(database_url),
        slot_name: "orders_crabka".to_owned(),
        publication_name: "crabka_connect".to_owned(),
        schema: "public".to_owned(),
        table_names: vec!["orders".to_owned()],
        max_messages_per_poll: 100,
    })
    .await?;
    let sink = KafkaSink::start(bootstrap, "db").await?;
    let checkpoints = Arc::new(KafkaCheckpointStore::start(bootstrap, CONNECTOR_ID).await?);
    Ok(ConnectorRuntime::<Bytes, Bytes>::new()
        .add_source(source)
        .add_sink(sink)
        .checkpoint_store(checkpoints)
        .max_batch(16)
        .commit_interval(Duration::from_millis(50))
        .poll_backoff(Duration::from_millis(20))
        .run())
}

async fn wait_for_running(handle: &ConnectorHandle) -> TestResult {
    timeout(WAIT, async {
        loop {
            match handle.state() {
                RuntimeState::Running => return Ok::<(), io::Error>(()),
                RuntimeState::Failed | RuntimeState::Stopped => {
                    return Err(io::Error::other(format!(
                        "connector stopped in state {:?}",
                        handle.state()
                    )));
                }
                RuntimeState::Starting | RuntimeState::Paused | RuntimeState::Draining => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connector did not start"))??;
    Ok(())
}

async fn wait_for_record_count(bootstrap: &str, expected: usize) -> TestResult {
    timeout(WAIT, async {
        loop {
            if let Ok(records) =
                crabka_replicator::admin_util::read_all(bootstrap, TOPIC, None).await
            {
                let count = records.len();
                assert!(
                    count <= expected,
                    "observed duplicate records: {count} > {expected}"
                );
                if count == expected {
                    return Ok::<(), io::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("topic did not reach {expected} records"),
        )
    })??;
    Ok(())
}

async fn read_records(
    bootstrap: &str,
    group_id: &str,
    expected: usize,
) -> TestResult<Vec<ObservedRecord>> {
    let mut consumer = timeout(
        WAIT,
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id)
            .client_id("crabka-connect-worker-acceptance")
            .subscribe(vec![TOPIC.to_owned()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build(),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "consumer did not start"))??;

    let records = timeout(WAIT, async {
        let mut records = Vec::with_capacity(expected);
        while records.len() < expected {
            for record in consumer.poll(millis(250)).await? {
                records.push(observe(record)?);
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(records)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "consumer did not receive records"))??;
    consumer.close().await?;
    Ok(records)
}

fn observe(record: ConsumerRecord) -> TestResult<ObservedRecord> {
    let operation = record
        .headers
        .iter()
        .find(|header| header.key == "crabka.pg.operation")
        .and_then(|header| header.value.as_deref())
        .ok_or_else(|| io::Error::other("CDC record is missing operation header"))?;
    Ok(ObservedRecord {
        key: record.key.map(|key| key.to_vec()),
        value: record.value.map(|value| value.to_vec()),
        operation: std::str::from_utf8(operation)?.to_owned(),
    })
}

fn encoded_key(id: i64) -> TestResult<Bytes> {
    Ok(PostgresProtoEncoder::new()?.encode_key(&EntityKey {
        table: "public.orders".to_owned(),
        columns: vec![ColumnValue {
            name: "id".to_owned(),
            value: ScalarValue::Int(id),
        }],
    })?)
}

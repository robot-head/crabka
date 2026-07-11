use clap::Parser;
use sqlx::{Connection as _, PgConnection};
use tokio_postgres::NoTls;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let database_url = args
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or("DATABASE_URL or --database-url is required")?;
    tokio_postgres_smoke(&database_url).await?;
    sqlx_smoke(&database_url).await?;
    println!("PASS: tokio-postgres and sqlx parameterized transaction-pooling smoke");
    Ok(())
}

async fn tokio_postgres_smoke(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);

    for expected in [41_i32, 42_i32] {
        let transaction = client.transaction().await?;
        let row = transaction
            .query_one("SELECT $1::int4", &[&expected])
            .await?;
        let actual: i32 = row.get(0);
        if actual != expected {
            return Err(format!("tokio-postgres returned {actual}, expected {expected}").into());
        }
        transaction.commit().await?;
    }

    drop(client);
    connection_task.await??;
    Ok(())
}

async fn sqlx_smoke(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = PgConnection::connect(database_url).await?;
    for expected in [51_i32, 52_i32] {
        let mut transaction = connection.begin().await?;
        let actual: i32 = sqlx::query_scalar("SELECT $1::int4")
            .bind(expected)
            .fetch_one(&mut *transaction)
            .await?;
        if actual != expected {
            return Err(format!("sqlx returned {actual}, expected {expected}").into());
        }
        transaction.commit().await?;
    }
    Ok(())
}

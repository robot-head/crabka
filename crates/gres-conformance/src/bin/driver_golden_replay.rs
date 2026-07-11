use clap::Parser;
use crabka_gres_conformance::driver_goldens::{parse_and_validate, replay_startup};
use tokio_postgres::NoTls;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long)]
    port: u16,
    #[arg(long, default_value = "crab")]
    user: String,
    #[arg(long, default_value = "crab")]
    dbname: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let fixture = parse_and_validate(
        include_str!("../../fixtures/driver-connect-v1.json"),
        include_str!("../../../../Cargo.lock"),
        include_str!("../../requirements-driver-smoke.txt"),
    )?;
    for capture in &fixture.drivers {
        replay_startup(
            &args.host,
            args.port,
            &args.user,
            &args.dbname,
            &capture.startup_parameters,
        )
        .await?;
    }

    let connection_string = format!(
        "host={} port={} user={} dbname={} sslmode=disable",
        args.host, args.port, args.user, args.dbname
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    for capture in &fixture.drivers {
        for batch in &capture.pgdog_backend_set_batches {
            client.batch_execute(batch).await?;
        }
    }
    drop(client);
    connection_task.await??;
    println!(
        "PASS: replayed 3 captured startups and all captured PgDog backend SET batches directly against Gres"
    );
    Ok(())
}

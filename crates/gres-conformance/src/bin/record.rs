//! TCP proxy that records `PostgreSQL` wire traffic to a trace file.
//! Usage: record --listen 127.0.0.1:54329 --upstream 127.0.0.1:54320 --out psql.trace

use std::{fmt::Write as _, sync::Arc};

use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    listen: String,
    #[arg(long)]
    upstream: String,
    #[arg(long)]
    out: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!(
        "recording {} -> {} into {}",
        args.listen,
        args.upstream,
        args.out.display()
    );
    let (client, _) = listener.accept().await?;
    let upstream = TcpStream::connect(&args.upstream).await?;
    let log = Arc::new(Mutex::new(String::new()));

    let (mut cr, mut cw) = client.into_split();
    let (mut ur, mut uw) = upstream.into_split();

    let log_f = Arc::clone(&log);
    let frontend = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = cr.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            append(&log_f, 'F', &buf[..n]).await;
            uw.write_all(&buf[..n]).await?;
        }
    });
    let log_b = Arc::clone(&log);
    let backend = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = ur.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            append(&log_b, 'B', &buf[..n]).await;
            cw.write_all(&buf[..n]).await?;
        }
    });
    let _ = frontend.await;
    let _ = backend.await;
    std::fs::write(&args.out, log.lock().await.as_str())?;
    eprintln!("wrote {}", args.out.display());
    Ok(())
}

async fn append(log: &Arc<Mutex<String>>, direction: char, bytes: &[u8]) {
    let mut line = String::with_capacity(bytes.len() * 2 + 3);
    let _ = write!(line, "{direction} ");
    for b in bytes {
        let _ = write!(line, "{b:02x}");
    }
    line.push('\n');
    log.lock().await.push_str(&line);
}

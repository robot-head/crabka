//! Test-only Kafka wire tap: a TCP relay that tees complete frames to a
//! `Recorder` while forwarding bytes byte-for-byte to a real broker.
pub mod frame;

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{Arc, Mutex},
    thread,
};

use frame::{CapturedFrame, Pending, parse_request_prefix, read_correlation_id};

/// Callback invoked once per fully-read frame in either direction.
pub type Recorder = Arc<dyn Fn(CapturedFrame) + Send + Sync>;

/// Bind a listener, accept connections, and relay each to `upstream`,
/// recording frames. Returns the bound local address (useful when the caller
/// passes port 0). The accept loop runs on a background thread for the
/// process lifetime.
pub fn spawn(
    listen: impl ToSocketAddrs,
    upstream: &str,
    recorder: Recorder,
) -> io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(listen)?;
    let addr = listener.local_addr()?;
    let upstream = upstream.to_string();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { continue };
            let upstream = upstream.clone();
            let recorder = recorder.clone();
            thread::spawn(move || {
                if let Err(e) = handle_conn(client, &upstream, recorder) {
                    eprintln!("tap conn error: {e}");
                }
            });
        }
    });
    Ok(addr)
}

fn handle_conn(client: TcpStream, upstream: &str, recorder: Recorder) -> io::Result<()> {
    let server = TcpStream::connect(upstream)?;
    let pending = Arc::new(Mutex::new(Pending::default()));

    let c2s_client = client.try_clone()?;
    let c2s_server = server.try_clone()?;
    let pend_req = pending.clone();
    let rec_req = recorder.clone();
    let t = thread::spawn(move || {
        let _ = pump(c2s_client, c2s_server, true, pend_req, rec_req);
    });

    pump(server, client, false, pending, recorder)?;
    let _ = t.join();
    Ok(())
}

/// Copy length-prefixed frames from `src` to `dst`, teeing each to the
/// recorder. `is_request` selects header parsing vs correlation lookup.
fn pump(
    mut src: TcpStream,
    mut dst: TcpStream,
    is_request: bool,
    pending: Arc<Mutex<Pending>>,
    recorder: Recorder,
) -> io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = src.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e);
        }
        let n = i32::from_be_bytes(len_buf);
        if n < 0 {
            return Ok(());
        }
        let mut body = vec![0u8; n as usize];
        src.read_exact(&mut body)?;
        dst.write_all(&len_buf)?;
        dst.write_all(&body)?;
        dst.flush()?;
        if is_request {
            if let Some(p) = parse_request_prefix(&body) {
                pending
                    .lock()
                    .unwrap()
                    .record(p.correlation_id, p.api_key, p.api_version);
                recorder(CapturedFrame {
                    api_key: p.api_key,
                    version: p.api_version,
                    is_request: true,
                    body,
                });
            }
        } else if let Some(corr) = read_correlation_id(&body)
            && let Some((api_key, version)) = pending.lock().unwrap().take(corr)
        {
            recorder(CapturedFrame {
                api_key,
                version,
                is_request: false,
                body,
            });
        }
    }
}

//! Standalone tap: `kafka-tap <listen> <upstream> <spool.ndjson>`.
//!
//! The tap writes one JSON record per frame to the spool file.
use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use crabka_kafka_tap::{Recorder, frame::CapturedFrame, spawn};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (listen, upstream, spool) = (&args[1], &args[2], args[3].clone());
    let file = Arc::new(Mutex::new(std::fs::File::create(&spool).unwrap()));
    let rec: Recorder = Arc::new(move |f: CapturedFrame| {
        let mut hex = String::with_capacity(f.body.len() * 2);
        for b in &f.body {
            use std::fmt::Write;
            let _ = write!(hex, "{b:02x}");
        }
        let line = format!(
            "{{\"api_key\":{},\"version\":{},\"is_request\":{},\"body_hex\":\"{}\"}}\n",
            f.api_key, f.version, f.is_request, hex
        );
        file.lock().unwrap().write_all(line.as_bytes()).unwrap();
    });
    let addr = spawn(listen.as_str(), upstream, rec).unwrap();
    eprintln!("kafka-tap listening on {addr} -> {upstream}, spooling to {spool}");
    loop {
        std::thread::park();
    }
}

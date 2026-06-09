//! End-to-end relay test against a fake upstream "broker" that echoes a
//! canned response. Verifies bytes pass through unmodified and frames are
//! recorded with correct classification.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::frame::CapturedFrame;
use crabka_kafka_tap::{Recorder, spawn};

fn framed(body: &[u8]) -> Vec<u8> {
    let mut v = (body.len() as i32).to_be_bytes().to_vec();
    v.extend_from_slice(body);
    v
}

#[test]
fn relays_and_records() {
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let up_addr = upstream.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut s, _) = upstream.accept().unwrap();
        let mut len = [0u8; 4];
        s.read_exact(&mut len).unwrap();
        let n = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; n];
        s.read_exact(&mut body).unwrap();
        let mut resp_body = 42i32.to_be_bytes().to_vec();
        resp_body.push(0x99);
        s.write_all(&framed(&resp_body)).unwrap();
    });

    let recorder = Arc::new(Mutex::new(Vec::<CapturedFrame>::new()));
    let rec_for_tap: Recorder = {
        let r = recorder.clone();
        Arc::new(move |f: CapturedFrame| r.lock().unwrap().push(f))
    };
    let tap_addr = spawn("127.0.0.1:0", &up_addr.to_string(), rec_for_tap).unwrap();

    let mut req_body = Vec::new();
    req_body.extend_from_slice(&3i16.to_be_bytes());
    req_body.extend_from_slice(&12i16.to_be_bytes());
    req_body.extend_from_slice(&42i32.to_be_bytes());
    req_body.push(0x11);

    let mut c = TcpStream::connect(tap_addr).unwrap();
    c.write_all(&framed(&req_body)).unwrap();
    let mut len = [0u8; 4];
    c.read_exact(&mut len).unwrap();
    let n = i32::from_be_bytes(len) as usize;
    let mut resp = vec![0u8; n];
    c.read_exact(&mut resp).unwrap();
    assert_eq!(resp, vec![0, 0, 0, 42, 0x99]);

    std::thread::sleep(std::time::Duration::from_millis(100));
    let frames = recorder.lock().unwrap().clone();
    assert!(
        frames
            .iter()
            .any(|f| f.is_request && f.api_key == 3 && f.version == 12)
    );
    assert!(
        frames
            .iter()
            .any(|f| !f.is_request && f.api_key == 3 && f.version == 12)
    );
}

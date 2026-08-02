use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{LazyLock, Mutex, MutexGuard},
};

use serde_json::{Value, json};

pub struct Oracle {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Oracle {
    pub fn spawn() -> Self {
        let mut cmd = if let Some(bin) = option_env!("CARGO_BIN_EXE_crabka-oracle") {
            Command::new(bin)
        } else {
            let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("tools/oracle/build/install/crabka-oracle");
            let bin = if cfg!(windows) {
                base.join("bin/crabka-oracle.bat")
            } else {
                base.join("bin/crabka-oracle")
            };
            assert2::assert!(bin.exists());
            if cfg!(windows) {
                Command::new(bin)
            } else {
                let mut command = Command::new("sh");
                command.arg(bin);
                command
            }
        };
        if let Some(java_home) = std::env::var_os("JAVA_HOME") {
            cmd.env("JAVA_HOME", java_home);
        } else if cfg!(windows) {
            cmd.env(
                "JAVA_HOME",
                r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot",
            );
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn oracle");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    pub fn call(&mut self, req: &Value) -> Value {
        let line = serde_json::to_string(req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        let mut resp = String::new();
        self.stdout.read_line(&mut resp).unwrap();
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert2::assert!(v["ok"].as_bool().unwrap_or(false));
        v
    }

    pub fn compress(&mut self, codec: &str, data: &[u8]) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "compress",
            "codec": codec,
            "data": hex::encode(data),
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }

    pub fn decompress(&mut self, codec: &str, data: &[u8]) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "decompress",
            "codec": codec,
            "data": hex::encode(data),
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static SHARED: LazyLock<Mutex<Oracle>> = LazyLock::new(|| Mutex::new(Oracle::spawn()));

/// Borrow the shared oracle. Tests serialize through the mutex so a single
/// JVM is reused across all differential cases.
pub fn shared() -> MutexGuard<'static, Oracle> {
    SHARED.lock().unwrap()
}

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
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tools/oracle/build/install/crabka-oracle");
        // Gradle's `installDist` produces BOTH wrappers on every platform
        // (the POSIX shell script and the .bat). Pick by host OS, not by
        // existence — picking by existence on Linux selects the .bat and
        // fails with ENOEXEC.
        let bin = if cfg!(windows) {
            base.join("bin/crabka-oracle.bat")
        } else {
            base.join("bin/crabka-oracle")
        };
        assert2::assert!(bin.exists());
        let java_home = std::env::var("JAVA_HOME").unwrap_or_else(|_| {
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot".to_string()
        });
        let mut cmd = if cfg!(windows) {
            Command::new(&bin)
        } else {
            let mut c = Command::new("sh");
            c.arg(&bin);
            c
        };
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("JAVA_HOME", java_home)
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

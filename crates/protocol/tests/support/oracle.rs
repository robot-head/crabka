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
        // Invoke the POSIX wrapper through `sh` rather than execve'ing it
        // directly. Gradle's `installDist` does not always preserve the
        // executable bit on the generated start script across CI runners,
        // and a non-executable script gets ENOEXEC from the kernel even
        // though the shebang is valid. Going through `sh` makes the spawn
        // independent of file mode bits.
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

    /// Like `call`, but returns Err with the oracle error message instead of a panic.
    #[allow(dead_code)]
    pub fn try_call(&mut self, req: &Value) -> Result<Value, String> {
        let line = serde_json::to_string(req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        let mut resp = String::new();
        self.stdout.read_line(&mut resp).unwrap();
        let v: Value = serde_json::from_str(&resp).unwrap();
        if v["ok"].as_bool().unwrap_or(false) {
            Ok(v)
        } else {
            Err(v["error"].as_str().unwrap_or("?").to_string())
        }
    }

    #[allow(dead_code)]
    pub fn encode(
        &mut self,
        api_key: i16,
        version: i16,
        is_request: bool,
        value: &Value,
    ) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "encode",
            "apiKey": api_key,
            "version": version,
            "isRequest": is_request,
            "value": value,
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }

    #[allow(dead_code)]
    pub fn decode(&mut self, api_key: i16, version: i16, is_request: bool, bytes: &[u8]) -> Value {
        let r = self.call(&json!({
            "op": "decode",
            "apiKey": api_key,
            "version": version,
            "isRequest": is_request,
            "hex": hex::encode(bytes),
        }));
        r["value"].clone()
    }

    #[allow(dead_code)]
    pub fn header_encode(&mut self, kind: &str, version: i16, value: &Value) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "header_encode",
            "kind": kind,
            "version": version,
            "value": value,
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }

    #[allow(dead_code)]
    pub fn header_decode(&mut self, kind: &str, version: i16, bytes: &[u8]) -> Value {
        let r = self.call(&json!({
            "op": "header_decode",
            "kind": kind,
            "version": version,
            "hex": hex::encode(bytes),
        }));
        r["value"].clone()
    }

    #[allow(dead_code)]
    pub fn record_batch_encode(&mut self, value: &Value) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "record_batch_encode",
            "value": value,
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }

    #[allow(dead_code)]
    pub fn record_batch_decode(&mut self, bytes: &[u8]) -> Value {
        let r = self.call(&json!({
            "op": "record_batch_decode",
            "hex": hex::encode(bytes),
        }));
        r["value"].clone()
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static SHARED: LazyLock<Mutex<Oracle>> = LazyLock::new(|| Mutex::new(Oracle::spawn()));

/// Borrow the shared oracle.
///
/// Tests serialize through the mutex, so all differential cases reuse a single
/// JVM.
pub fn shared() -> MutexGuard<'static, Oracle> {
    SHARED.lock().unwrap()
}

use std::sync::Arc;

use crabka_pgwire::{
    session::{AuthMode, SessionConfig},
    stub::StubEngine,
};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

fn fixture_password(prefix: &str, suffix: &str) -> String {
    prefix.to_owned() + suffix
}

fn scram_config() -> SessionConfig {
    use crabka_pgwire::scram::ScramVerifier;
    let mut verifiers = std::collections::HashMap::new();
    verifiers.insert(
        "crab".to_string(),
        ScramVerifier::from_password(&fixture_password("hunter", "2"), vec![7u8; 16], 4096),
    );
    SessionConfig {
        auth: AuthMode::ScramSha256 {
            verifiers,
            mock_secret: [42u8; 32],
        },
        ..SessionConfig::trust()
    }
}

async fn spawn_scram_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let config = scram_config();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(config),
    ));
    port
}

fn frame_len(len: usize) -> i32 {
    i32::try_from(len).expect("test frame length fits in i32") + 4
}

#[tokio::test]
async fn correct_password_authenticates_and_queries() {
    let port = spawn_scram_server().await;
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password(fixture_password("hunter", "2"))
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("scram connect");
    tokio::spawn(conn);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn wrong_password_is_rejected_with_28p01() {
    let port = spawn_scram_server().await;
    let result = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password(fixture_password("wr", "ong"))
        .dbname("crab")
        .connect(NoTls)
        .await;
    let err = result.map(|_| ()).expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "28P01");
}

#[tokio::test]
async fn unknown_user_is_rejected() {
    let port = spawn_scram_server().await;
    let result = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("mallory")
        .password(fixture_password("what", "ever"))
        .dbname("crab")
        .connect(NoTls)
        .await;
    let err = result
        .map(|_| ())
        .expect_err("unknown user must be rejected");
    let db = err
        .as_db_error()
        .expect("must be a db protocol error with code");
    assert_eq!(
        db.code().code(),
        "28P01",
        "unknown user must yield 28P01, not a different error"
    );
}

#[tokio::test]
async fn mock_salt_length_matches_real_user() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn read_msg(c: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        c.read_exact(&mut header)
            .await
            .expect("read message header");
        let len = usize::try_from(i32::from_be_bytes([
            header[1], header[2], header[3], header[4],
        ]))
        .expect("backend frame length is non-negative");
        let mut payload = vec![0u8; len - 4];
        c.read_exact(&mut payload)
            .await
            .expect("read message payload");
        (header[0], payload)
    }

    async fn salt_b64_len(port: u16, user: &str) -> usize {
        let mut c = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp connect");

        // StartupMessage: int32 length + int32 protocol (196608 = 3<<16) + params + terminator
        let mut body = Vec::new();
        body.extend_from_slice(&196_608i32.to_be_bytes());
        body.extend_from_slice(format!("user\0{user}\0database\0db\0\0").as_bytes());
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&frame_len(body.len()).to_be_bytes());
        pkt.extend_from_slice(&body);
        c.write_all(&pkt).await.expect("send startup");

        // Expect AuthenticationSASL ('R' with code 10) — read and discard
        let (tag, _) = read_msg(&mut c).await;
        assert_eq!(tag, b'R', "expected AuthenticationSASL message");

        // SASLInitialResponse ('p'): mechanism + int32 len of client-first + client-first bytes
        let client_first = format!("n,,n={user},r=CNONCE");
        let mut b = Vec::new();
        b.extend_from_slice(b"SCRAM-SHA-256\0");
        b.extend_from_slice(
            &i32::try_from(client_first.len())
                .expect("client-first length fits in i32")
                .to_be_bytes(),
        );
        b.extend_from_slice(client_first.as_bytes());
        let mut m = Vec::new();
        m.push(b'p');
        m.extend_from_slice(&frame_len(b.len()).to_be_bytes());
        m.extend_from_slice(&b);
        c.write_all(&m).await.expect("send sasl initial response");

        // Read AuthenticationSASLContinue ('R', int32 code=11, then server-first text)
        let (tag, payload) = read_msg(&mut c).await;
        assert_eq!(tag, b'R', "expected AuthenticationSASLContinue tag");
        let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        assert_eq!(code, 11, "expected AuthenticationSASLContinue (code 11)");
        let server_first = std::str::from_utf8(&payload[4..]).expect("server-first must be UTF-8");
        let s = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("s="))
            .expect("s= field must be present in server-first");
        s.len()
    }

    let port = spawn_scram_server().await;
    let known_len = salt_b64_len(port, "crab").await;
    let unknown_len = salt_b64_len(port, "ghost").await;
    assert_eq!(
        known_len, unknown_len,
        "known user s= length ({known_len}) must equal unknown user s= length ({unknown_len}): \
         differing lengths leak user existence via SCRAM server-first message"
    );
}

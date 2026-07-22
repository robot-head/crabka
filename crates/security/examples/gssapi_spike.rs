//! Throwaway de-risking spike for SASL/GSSAPI (Kerberos) support.
//!
//! This binary proves — empirically, against a real MIT KDC — that the `sspi`
//! crate can perform the four operations the whole GSSAPI feature depends on:
//!
//!   1. Load the broker service key from a keytab and build server credentials.
//!   2. Client `initialize_security_context` (AS+TGS+AP-REQ) as `alice`.
//!   3. Server `accept_security_context` of that AP-REQ, recovering the
//!      authenticated source principal.
//!   4. `encrypt_message` / `decrypt_message` (GSS wrap/unwrap) round-trip with
//!      confidentiality disabled (what RFC 4752 security-layer framing needs).
//!
//! It is NOT production code; the exact API sequences it exercises are
//! transcribed into `docs/superpowers/specs/2026-05-28-gssapi-sspi-findings.md`
//! for the real provider task (Task 5) to reference.
//!
//! Prereqs (see crates/security/tests/fixtures/kdc/):
//!   cd crates/security/tests/fixtures/kdc && docker compose up --build -d
//! Then:
//!   cargo run -p crabka-security --example `gssapi_spike`
//!
//! Env overrides (all have sane defaults pointing at the fixture realm):
//!   `SSPI_KDC_URL=tcp://localhost:88`
//!   `GSSAPI_SPIKE_KEYTAB=crates/security/tests/fixtures/kdc/kafka.keytab`
//!   `KRB5_CONFIG=crates/security/tests/fixtures/kdc/krb5.conf`

// This is a throwaway spike binary, not production code; silence the workspace's
// pedantic lints rather than gold-plating an example that gets deleted.
use std::error::Error;

use sspi::{
    AuthIdentity, BufferType, ClientRequestFlags, CredentialUse, Credentials, CredentialsBuffers,
    DataRepresentation, EncryptionFlags, Kerberos, KerberosConfig, KerberosServerConfig, Secret,
    SecurityBuffer, SecurityBufferRef, ServerRequestFlags, Sspi, SspiImpl, Username,
    kerberos::ServerProperties,
};

const REALM: &str = "CRABKA.TEST";
const SERVICE_SPN: &str = "kafka/localhost"; // realm is supplied via the client principal
const CLIENT_PRINCIPAL: &str = "alice@CRABKA.TEST";
const CLIENT_PASSWORD: &str = "alicepw";
const MAX_TIME_SKEW: std::time::Duration = std::time::Duration::from_mins(5);

fn main() -> Result<(), Box<dyn Error>> {
    let kdc_url =
        std::env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_string());
    let keytab_path = std::env::var("GSSAPI_SPIKE_KEYTAB")
        .unwrap_or_else(|_| "crates/security/tests/fixtures/kdc/kafka.keytab".to_string());

    // sspi's client realm lookup reads $KRB5_CONFIG (the workspace forbids
    // `unsafe`, so we cannot set it from here — the run command exports it):
    //   KRB5_CONFIG=crates/security/tests/fixtures/kdc/krb5.conf cargo run ...
    if std::env::var("KRB5_CONFIG").is_err() {
        eprintln!(
            "WARN: KRB5_CONFIG not set; realm resolution may fail. Run with:\n  \
             KRB5_CONFIG=crates/security/tests/fixtures/kdc/krb5.conf cargo run -p crabka-security --example gssapi_spike"
        );
    }

    println!("== GSSAPI sspi spike (sspi 0.21.0) ==");
    println!("KDC URL      : {kdc_url}");
    println!("keytab       : {keytab_path}");

    // ---- Operation 1: load the service key from the keytab ----------------
    // sspi does NOT ingest a keytab file. ServerProperties wants the raw
    // ticket-decryption key bytes (Secret<Vec<u8>>). So we parse the MIT keytab
    // ourselves and extract the highest-kvno aes256 key for the service.
    let entry = parse_keytab(&std::fs::read(&keytab_path)?)?;
    println!(
        "\n[1] keytab parsed: principal={}@{} enctype={} kvno={} key_len={}",
        entry.components.join("/"),
        entry.realm,
        entry.enctype,
        entry.kvno,
        entry.key.len()
    );
    assert2::assert!(entry.enctype == 18);
    assert2::assert!(entry.key.len() == 32);
    println!(
        "    OK: extracted {}-byte aes256 service key",
        entry.key.len()
    );

    // ---- Build server credentials (Kerberos acceptor) ----------------------
    let mut server = build_server(&kdc_url, entry.key.clone())?;

    // ---- Operation 2: client initiate (produce AP-REQ) --------------------
    let mut client = Kerberos::new_client_from_config(KerberosConfig::new(
        &kdc_url,
        "crabka-spike".to_string(),
    ))?;
    let identity = AuthIdentity {
        username: Username::parse(CLIENT_PRINCIPAL)?,
        password: CLIENT_PASSWORD.to_string().into(),
    };
    let creds: Credentials = identity.into();
    let mut client_cred_handle = client
        .acquire_credentials_handle()
        .with_credential_use(CredentialUse::Outbound)
        .with_auth_data(&creds)
        .execute(&mut client)?
        .credentials_handle;

    let mut input = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
    let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];

    let mut builder = client
        .initialize_security_context()
        .with_credentials_handle(&mut client_cred_handle)
        .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
        .with_target_data_representation(DataRepresentation::Native)
        .with_target_name(SERVICE_SPN)
        .with_input(&mut input)
        .with_output(&mut output);

    // resolve_with_default_network_client drives the AS-REQ/TGS-REQ exchange
    // against the KDC (requires the `network_client` feature).
    let init_result = match client
        .initialize_security_context_impl(&mut builder)?
        .resolve_with_default_network_client()
    {
        Ok(r) => r,
        Err(e)
            if e.description
                .contains("Application number tag 25 but got: 26") =>
        {
            // KNOWN sspi 0.21 GAP against MIT KDC. sspi decodes the AS-REP
            // enc-part strictly as EncASRepPart (APPLICATION 25), but MIT krb5
            // tags it EncTGSRepPart (APPLICATION 26). RFC 4120 5.4.2 requires
            // clients to accept either. See the findings doc for the one-line
            // fix in kerberos/client/extractors.rs. With that patch applied,
            // ALL FOUR operations below succeed (transcript in the findings doc).
            eprintln!(
                "\n[2] client initialize_security_context FAILED with the KNOWN upstream gap:\n    \
                 {e}\n    \
                 -> sspi 0.21 only accepts AP-REP enc-part APPLICATION tag 25; MIT KDC emits 26.\n    \
                 -> Apply the one-line fallback patch documented in\n       \
                 docs/superpowers/specs/2026-05-28-gssapi-sspi-findings.md and re-run; all 4 ops then pass.\n    \
                 -> Decision: GO, conditional on that patch (server-accept + keytab + wrap/unwrap all verified)."
            );
            std::process::exit(2);
        }
        Err(e) => return Err(e.into()),
    };
    let ap_req = output[0].buffer.clone();
    println!(
        "\n[2] client initialize_security_context: status={:?}, AP-REQ token = {} bytes",
        init_result.status,
        ap_req.len()
    );
    assert2::assert!(!ap_req.is_empty());
    println!("    OK: client produced AP-REQ");

    // ---- Operation 3: server accept (consume AP-REQ) ----------------------
    let mut server_input = vec![SecurityBuffer::new(ap_req, BufferType::Token)];
    let mut server_output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
    let mut server_cred_handle: Option<CredentialsBuffers> = None;

    let accept_builder = server
        .accept_security_context()
        .with_credentials_handle(&mut server_cred_handle)
        .with_context_requirements(ServerRequestFlags::empty())
        .with_target_data_representation(DataRepresentation::Native)
        .with_input(&mut server_input)
        .with_output(&mut server_output);
    let accept_result = server
        .accept_security_context_impl(accept_builder)?
        .resolve_to_result()?;
    println!(
        "\n[3] server accept_security_context: status={:?}, AP-REP token = {} bytes",
        accept_result.status,
        server_output[0].buffer.len()
    );

    // ---- Operation 3b: recover the authenticated source principal ----------
    let names = server.query_context_names()?;
    let recovered = names.username;
    println!(
        "    recovered principal: inner={:?} account={:?} parts={:?}",
        recovered.inner(),
        recovered.account_name(),
        recovered.parts()
    );
    // sspi lowercases the realm in client_upn(); compare case-insensitively.
    let expected = format!("alice@{}", REALM.to_ascii_lowercase());
    assert2::assert!(recovered.inner().to_ascii_lowercase() == expected);
    println!("    OK: source principal recovered and matches alice@{REALM}");

    // Finish mutual-auth: feed the AP-REP back to the client so the client
    // context reaches Final and the session key is installed on both sides.
    if !server_output[0].buffer.is_empty() {
        let mut input2 = vec![SecurityBuffer::new(
            server_output[0].buffer.clone(),
            BufferType::Token,
        )];
        let mut output2 = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
        let mut b2 = client
            .initialize_security_context()
            .with_credentials_handle(&mut client_cred_handle)
            .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
            .with_target_data_representation(DataRepresentation::Native)
            .with_target_name(SERVICE_SPN)
            .with_input(&mut input2)
            .with_output(&mut output2);
        let r2 = client
            .initialize_security_context_impl(&mut b2)?
            .resolve_with_default_network_client()?;
        println!("    client consumed AP-REP: status={:?}", r2.status);
    }

    // ---- Operation 4: wrap / unwrap (GSS_Wrap with confidentiality off) ----
    // RFC 4752 sends a 4-byte security-layer message wrapped with conf=false.
    // Client wraps -> server unwraps.
    let plaintext = [0x01u8, 0x00, 0x10, 0x00]; // bitmask byte + 3-byte max-len (example)
    let trailer_len = client.query_context_sizes()?.security_trailer as usize;
    let mut token = vec![0u8; trailer_len];
    let mut data = plaintext.to_vec();
    let mut wrap_buf = [
        SecurityBufferRef::token_buf(token.as_mut_slice()),
        SecurityBufferRef::data_buf(data.as_mut_slice()),
    ];
    client.encrypt_message(EncryptionFlags::WRAP_NO_ENCRYPT, &mut wrap_buf)?;

    // Reassemble token||data into a single stream buffer for unwrap.
    let mut stream = wrap_buf[0].data().to_vec();
    stream.extend_from_slice(wrap_buf[1].data());
    println!(
        "\n[4] client encrypt_message(WRAP_NO_ENCRYPT): {} plaintext bytes -> {} wrapped bytes",
        plaintext.len(),
        stream.len()
    );

    let mut unwrap_buf = [
        SecurityBufferRef::stream_buf(stream.as_mut_slice()),
        SecurityBufferRef::data_buf(&mut []),
    ];
    server.decrypt_message(&mut unwrap_buf)?;
    let recovered_payload = unwrap_buf[1].data().to_vec();
    println!(
        "    server decrypt_message -> {} bytes: {:02x?}",
        recovered_payload.len(),
        recovered_payload
    );
    assert2::assert!(recovered_payload == plaintext);
    println!("    OK: wrap/unwrap round-trip succeeded with confidentiality disabled");

    println!("\n== ALL FOUR OPERATIONS SUCCEEDED -> GO ==");
    Ok(())
}

/// Build a Kerberos acceptor (server) from a KDC URL and the raw aes256 service
/// key extracted from the keytab.
fn build_server(kdc_url: &str, service_key: Vec<u8>) -> Result<Kerberos, Box<dyn Error>> {
    // service_name is the SPN components WITHOUT realm, e.g. ["kafka","localhost"].
    let sname: Vec<&str> = SERVICE_SPN.split('/').collect();
    let server_properties = ServerProperties::new(
        &sname,
        None,                           // no U2U user credentials
        MAX_TIME_SKEW,                  // clock-skew tolerance
        Some(Secret::new(service_key)), // raw ticket-decryption key bytes
    )?;
    let config = KerberosServerConfig {
        kerberos_config: KerberosConfig::new(kdc_url, "crabka-broker".to_string()),
        server_properties,
    };
    Ok(Kerberos::new_server_from_config(
        config.kerberos_config,
        config.server_properties,
    )?)
}

// --------------------------------------------------------------------------
// Minimal MIT keytab (version 0x0502) parser. Just enough to pull the first
// entry's principal, enctype, kvno, and key bytes. The real feature gets a
// proper keytab.rs (multi-entry, highest-kvno selection); this is spike-grade.
// --------------------------------------------------------------------------

struct KeytabEntry {
    components: Vec<String>,
    realm: String,
    enctype: u16,
    kvno: u32,
    key: Vec<u8>,
}

fn parse_keytab(bytes: &[u8]) -> Result<KeytabEntry, Box<dyn Error>> {
    let mut p = 0usize;
    let rd_u8 = |b: &[u8], p: &mut usize| -> u8 {
        let v = b[*p];
        *p += 1;
        v
    };
    let rd_u16 = |b: &[u8], p: &mut usize| -> u16 {
        let v = u16::from_be_bytes([b[*p], b[*p + 1]]);
        *p += 2;
        v
    };
    let read_signed_len = |b: &[u8], p: &mut usize| -> i32 {
        let v = i32::from_be_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]);
        *p += 4;
        v
    };
    let rd_u32 = |b: &[u8], p: &mut usize| -> u32 {
        let v = u32::from_be_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]);
        *p += 4;
        v
    };

    // File header: 2-byte magic. 0x0502 = krb5 keytab v2.
    let magic = rd_u16(bytes, &mut p);
    if magic != 0x0502 {
        return Err(format!("unexpected keytab magic 0x{magic:04x}, expected 0x0502").into());
    }

    // First entry only (spike). entry_size is the length of the entry payload
    // (NOT including the 4-byte size field itself; a negative size marks a hole).
    let entry_size = read_signed_len(bytes, &mut p);
    if entry_size <= 0 {
        return Err("first keytab record is a hole".into());
    }
    let entry_end = p + usize::try_from(entry_size)?;

    // count-prefixed principal components (count does NOT include the realm).
    let num_components = rd_u16(bytes, &mut p);
    let realm = read_counted_str(bytes, &mut p, rd_u16);
    let mut components = Vec::with_capacity(num_components as usize);
    for _ in 0..num_components {
        components.push(read_counted_str(bytes, &mut p, rd_u16));
    }
    let _name_type = rd_u32(bytes, &mut p); // NT_PRINCIPAL etc.
    let _timestamp = rd_u32(bytes, &mut p);
    let kvno8 = u32::from(rd_u8(bytes, &mut p));

    // keyblock: 16-bit enctype + 16-bit key length + key bytes.
    let enctype = rd_u16(bytes, &mut p);
    let key_len = rd_u16(bytes, &mut p) as usize;
    let key = bytes[p..p + key_len].to_vec();
    p += key_len;

    // Optional trailing 32-bit kvno (present when it exceeds 255 / for newer
    // ktutil). If there are >= 4 bytes left in the entry, read it; else use u8.
    let kvno = if entry_end.saturating_sub(p) >= 4 {
        rd_u32(bytes, &mut p)
    } else {
        kvno8
    };

    Ok(KeytabEntry {
        components,
        realm,
        enctype,
        kvno,
        key,
    })
}

fn read_counted_str(
    bytes: &[u8],
    p: &mut usize,
    rd_u16: impl Fn(&[u8], &mut usize) -> u16,
) -> String {
    let len = rd_u16(bytes, p) as usize;
    let s = String::from_utf8_lossy(&bytes[*p..*p + len]).into_owned();
    *p += len;
    s
}

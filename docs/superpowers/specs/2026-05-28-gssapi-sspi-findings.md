# GSSAPI / `sspi` spike findings (Task 1 — GATE)

**Date:** 2026-05-28
**Author:** spike run on the worktree branch
**Status:** **GO — conditional on a documented one-line `sspi` patch** (see §6)

This is the de-risking spike for SASL/GSSAPI (Kerberos). It proves, empirically
against a real MIT KDC (krb5 1.20.1), that the `sspi` crate can perform the four
operations the whole feature depends on. The downstream provider task (Task 5)
should treat the API sequences below as copy-pasteable.

---

## 0. TL;DR / decision gate

| # | Operation | Result |
|---|-----------|--------|
| 1 | Load service key from keytab → server credentials | **WORKS** (needs our own keytab parser — see §5) |
| 2 | Client `initialize_security_context` (AS+TGS+AP-REQ) | **WORKS, but only with the §6 patch** (upstream sspi has an MIT-KDC interop bug) |
| 3 | Server `accept_security_context` + recover source principal | **WORKS** (the original biggest risk — fully implemented in sspi) |
| 4 | `encrypt_message` / `decrypt_message` (GSS wrap/unwrap, conf off) | **WORKS** |

**Decision: GO.** The server-accept path — the single biggest unknown going in —
is fully implemented in sspi and works first-try. The only blocker is a trivial,
RFC-justified strictness bug on the *client* AS-REP decode that a one-line fork
patch fixes. With that patch, the spike runs end-to-end and prints
`== ALL FOUR OPERATIONS SUCCEEDED -> GO ==`.

Two integration costs the plan did not anticipate, both documented below:
- The workspace `pbkdf2` dependency must move to `=0.13.0-rc.10` (§2).
- `sspi` needs the one-line AS-REP tag patch (§6). Task 5 must decide: carry a
  small fork, vendor the crate, or upstream the fix to devolutions/sspi-rs.

---

## 1. Resolved versions

The plan said `sspi = "0.16"`, but **`sspi 0.16.x` does not compile in this
workspace** (its `picky 7.0.0-rc.23` transitive needs a `crypto-bigint`/`rsa`
that conflicts with Crabka's stable RustCrypto stack). Per the plan's
instruction to "pin to the latest 0.x that resolves at build time", we use:

```toml
# crates/security/Cargo.toml
# `ring` instead of the default `aws-lc-rs` to match the rest of the workspace.
sspi = { version = "0.21", default-features = false, features = ["network_client", "ring"] }
```

Resolved at build time (from `Cargo.lock`):

| crate | version | notes |
|-------|---------|-------|
| `sspi` | **0.21.0** | `checksum 3db83308…0fb0` |
| `picky-krb` | 0.12.3 | the Kerberos guts sspi rides on |
| `picky` | 7.0.0-rc.23 | (RC — upstream has not shipped a stable picky 7) |
| `pbkdf2` | **0.13.0-rc.10** | forced by `picky-krb 0.12.3`; see §2 |

The `network_client` feature pulls in the reqwest-based KDC transport used by
`resolve_with_default_network_client()` (the client AS/TGS exchange).

Versions probed and rejected:
- `0.16.1` — `picky 7.0.0-rc.23` vs workspace `rsa`/`crypto-bigint`: compile error.
- `0.17.0` — resolves but sspi's own source fails to compile against the
  `picky-krb 0.11.1` / `rand_core 0.6.4` the workspace lock forces on it
  (`StdRng::next_u32`, missing `KerberosCryptoError` variants, etc.).
- `0.18–0.20` — `hmac =0.13.0-rc.3` pre-release pin conflicts with workspace
  `hmac 0.13.0`.
- `0.21.0` — resolves and compiles **once** `pbkdf2` is bumped (§2).

---

## 2. Workspace dependency change required (`pbkdf2`)

`sspi 0.21 → picky-krb 0.12.3` hard-pins `pbkdf2 = "=0.13.0-rc.10"` (a
pre-release). Crabka's workspace pins stable `pbkdf2 = "0.13"` (→ `0.13.0`),
used by SCRAM and delegation tokens. Cargo cannot unify stable `0.13.0` with the
pinned RC, so it backtracks `picky-krb` to `0.12.0`, which then pins
`crypto-bigint =0.7.0-rc.8` — a dead-end conflict with sspi's own
`crypto-bigint ^0.7`.

**Fix (root `Cargo.toml`):**

```toml
# was: pbkdf2 = { version = "0.13", default-features = false, features = ["kdf", "hmac"] }
pbkdf2 = { version = "=0.13.0-rc.10", default-features = false, features = ["kdf", "hmac"] }
```

Verified: with this bump the **full workspace builds** and `crabka-security`'s
137 existing tests (incl. SCRAM, which uses pbkdf2) all pass. The RC is API- and
output-compatible for our PBKDF2-HMAC-SHA-256/512 usage.

> NOTE: This pbkdf2 bump **is committed** as part of the spike, because sspi 0.21
> cannot resolve in the workspace without it (the conflict above is a hard
> resolution failure, not a runtime one). The spike example is committed against
> **upstream** sspi (no §6 patch), so it compiles in CI but fails op 2 at the §6
> gap — detecting that exact error and printing the GO-conditional message. To
> reproduce the full end-to-end GO run, additionally apply the §6 patch via
> `[patch.crates-io]` (the spike was validated with the patch in place).

---

## 3. Local KDC test realm

`crates/security/tests/fixtures/kdc/` hand-rolls an MIT krb5 1.20.1 KDC
(`debian:bookworm-slim` + `krb5-kdc krb5-admin-server krb5-user`) for realm
`CRABKA.TEST`.

```bash
cd crates/security/tests/fixtures/kdc
docker compose up --build -d      # realm up; kafka.keytab + alice.keytab land here
docker compose logs | grep READY  # "KDC_READY" once setup + smoke-test kinit pass
# ... run the spike ...
docker compose down
```

Port `88/tcp+udp` is mapped to `localhost` so the host-side sspi client reaches
the KDC via `SSPI_KDC_URL=tcp://localhost:88`.

Principal / keytab creation (`setup.sh`, runs inside the container):

```bash
kdb5_util create -r CRABKA.TEST -s -P masterkey
kadmin.local -q "addprinc -randkey kafka/localhost@CRABKA.TEST"   # broker service
kadmin.local -q "addprinc -pw alicepw alice@CRABKA.TEST"          # client
# CRITICAL: sspi's client AS-exchange requires the KDC to DEMAND pre-auth
# (it sends a no-preauth AS-REQ expecting KDC_ERR_PREAUTH_REQUIRED to learn the
# salt, then resends with the encrypted timestamp). Without this, the KDC issues
# an AS-REP on the first try and sspi errors:
#   "KDC server should not process AS_REQ without the pa-pac data"
kadmin.local -q "modprinc +requires_preauth alice@CRABKA.TEST"
# Export keytabs into the shared /fixtures volume (= this dir on the host):
kadmin.local -q "ktadd -k /fixtures/kafka.keytab -norandkey kafka/localhost@CRABKA.TEST"
kadmin.local -q "ktadd -k /fixtures/alice.keytab  -norandkey alice@CRABKA.TEST"
# Smoke test (after krb5kdc is up):
kinit -kt /fixtures/kafka.keytab kafka/localhost@CRABKA.TEST   # succeeds
```

Enctype is pinned to `aes256-cts-hmac-sha1-96` (enctype 18) in both `krb5.conf`
and `kdc.conf` — the default Kerberos enctype that picky-krb supports.

Realm casing matters: the SPN/realm is `CRABKA.TEST` (upper); sspi lowercases the
realm when it recovers the client principal (see §4d).

---

## 4. Exact, working API sequences (sspi 0.21)

Imports used throughout:

```rust
use sspi::kerberos::ServerProperties;
use sspi::{
    AuthIdentity, BufferType, ClientRequestFlags, CredentialUse, Credentials, CredentialsBuffers,
    DataRepresentation, EncryptionFlags, Kerberos, KerberosConfig, KerberosServerConfig, Secret,
    SecurityBuffer, SecurityBufferRef, ServerRequestFlags, Sspi, SspiImpl, Username,
};
```

### 4a. Build server (acceptor) credentials from the keytab key

sspi does **not** ingest a keytab file. The server key is supplied as the raw
ticket-decryption key bytes via `ServerProperties` (see §5 for extraction).

```rust
// `service_key`: 32 raw aes256 key bytes pulled from the keytab.
// `sname`: SPN components WITHOUT realm, e.g. ["kafka", "localhost"].
let sname: Vec<&str> = "kafka/localhost".split('/').collect();
let server_properties = ServerProperties::new(
    &sname,
    None,                                      // U2U user creds — not used for broker accept
    std::time::Duration::from_secs(300),       // max clock skew
    Some(Secret::new(service_key)),            // <-- the raw service key bytes
)?;
let mut server = Kerberos::new_server_from_config(
    KerberosConfig::new("tcp://localhost:88", "crabka-broker".to_string()),
    server_properties,
)?;
```

`ServerProperties` field of interest:
`ticket_decryption_key: Option<Secret<Vec<u8>>>` — exactly the bytes from §5.

### 4b. Client `initialize_security_context` (produces AP-REQ)

```rust
let mut client =
    Kerberos::new_client_from_config(KerberosConfig::new("tcp://localhost:88", "crabka-spike".to_string()))?;

// Client principal + secret. The realm is derived from the UPN suffix
// ("alice@CRABKA.TEST" -> realm CRABKA.TEST) via $KRB5_CONFIG lookup, so
// KRB5_CONFIG must point at a krb5.conf that knows the realm.
let identity = AuthIdentity {
    username: Username::parse("alice@CRABKA.TEST")?,
    password: "alicepw".to_string().into(),
};
let creds: Credentials = identity.into();
let mut client_cred_handle = client
    .acquire_credentials_handle()
    .with_credential_use(CredentialUse::Outbound)
    .with_auth_data(&creds)
    .execute(&mut client)?
    .credentials_handle;

let mut input  = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];

let mut builder = client
    .initialize_security_context()
    .with_credentials_handle(&mut client_cred_handle)
    .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
    .with_target_data_representation(DataRepresentation::Native)
    .with_target_name("kafka/localhost")        // SPN; realm comes from the client principal
    .with_input(&mut input)
    .with_output(&mut output);

// Drives the AS-REQ / TGS-REQ round-trips against the KDC.
// Requires the `network_client` feature.
let result = client
    .initialize_security_context_impl(&mut builder)?
    .resolve_with_default_network_client()?;     // <-- fails here on upstream; see §6
let ap_req: Vec<u8> = output[0].buffer.clone();  // 747 bytes in the spike run
// result.status == SecurityStatus::ContinueNeeded (MUTUAL_AUTH wants the AP-REP back)
```

### 4c. Server `accept_security_context` (consumes AP-REQ)

```rust
let mut server_input  = vec![SecurityBuffer::new(ap_req, BufferType::Token)];
let mut server_output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
// The acceptor cred handle is just None — the service key lives in ServerProperties.
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
// accept_result.status == SecurityStatus::Ok
// server_output[0].buffer == AP-REP (157 bytes; present because MUTUAL_REQUIRED)
```

### 4d. Recover the authenticated source principal

After a successful accept, the client principal is exposed via
`query_context_names()` (which returns `ServerProperties.client`):

```rust
let names = server.query_context_names()?;
let user: Username = names.username;
user.inner();          // "alice@crabka.test"   (NOTE: realm is LOWERCASED by sspi)
user.account_name();   // "alice"
user.domain_name();    // Some("crabka.test")
```

> Casing: sspi's `client_upn()` does `crealm.to_ascii_lowercase()`, so the realm
> comes back lowercased. The `auth_to_local` task (Task 3) must normalize realm
> case (compare case-insensitively, or upper-case before matching rules).

### 4e. Wrap / unwrap (GSS_Wrap, confidentiality OFF — RFC 4752 layer)

RFC 4752's security-layer negotiation sends a 4-byte token wrapped with
confidentiality disabled. Use `EncryptionFlags::WRAP_NO_ENCRYPT` and the
`SecurityBufferRef` token+data layout; unwrap with a single stream buffer.

```rust
let plaintext = [0x01u8, 0x00, 0x10, 0x00];                      // bitmask + 24-bit max len
let trailer_len = client.query_context_sizes()?.security_trailer as usize;
let mut token = vec![0u8; trailer_len];
let mut data  = plaintext.to_vec();
let mut wrap_buf = [
    SecurityBufferRef::token_buf(token.as_mut_slice()),
    SecurityBufferRef::data_buf(data.as_mut_slice()),
];
client.encrypt_message(EncryptionFlags::WRAP_NO_ENCRYPT, &mut wrap_buf)?;

// Reassemble token||data into one stream buffer for unwrap:
let mut stream = wrap_buf[0].data().to_vec();
stream.extend_from_slice(wrap_buf[1].data());                    // 64 bytes total in the run

let mut unwrap_buf = [
    SecurityBufferRef::stream_buf(stream.as_mut_slice()),
    SecurityBufferRef::data_buf(&mut []),
];
server.decrypt_message(&mut unwrap_buf)?;
assert_eq!(unwrap_buf[1].data(), plaintext);                     // round-trip OK
```

(Mutual auth: feed the AP-REP from §4c back into a second client
`initialize_security_context` call with the same target name so the client
context reaches `Final` and installs the session key before wrapping. See the
spike example for the exact second-leg call.)

---

## 5. Keytab parsing IS required

`sspi` takes the **raw service key bytes**, not a keytab path. So a
`keytab.rs` parser (Task 2/5) must extract `(enctype, key_bytes)` from the MIT
keytab. Confirmed MIT keytab layout (`kafka.keytab`, version `0x0502`):

```
file header:
  u16  magic            = 0x0502
per entry:
  i32  entry_size       (length of the rest of the entry; negative => hole, skip)
  u16  num_components    (count of principal name components; EXCLUDES realm)
  u16 len + bytes        realm                ("CRABKA.TEST")
  { u16 len + bytes }*   components            ("kafka", "localhost")
  u32  name_type         (1 = NT_PRINCIPAL)
  u32  timestamp
  u8   kvno8
  keyblock:
    u16  enctype         (18 = aes256-cts-hmac-sha1-96)
    u16  key_length      (32 for aes256)
    [u8; key_length]     key bytes            <-- this is ServerProperties.ticket_decryption_key
  u32  kvno32            (OPTIONAL; present iff >= 4 bytes remain in the entry;
                          overrides kvno8 when present)
```

Real-world keytabs hold multiple entries (one per enctype/kvno). The parser must
iterate entries, skip holes (negative `entry_size`), and select the entry
matching the broker SPN with the **highest kvno** for the negotiated enctype.
The spike used a minimal first-entry-only parser (see
`crates/security/examples/gssapi_spike.rs::parse_keytab`).

Verified hexdump of the spike's `kafka.keytab`:
`0502 0000 0052 0002 000b "CRABKA.TEST" 0005 "kafka" 0009 "localhost"
00000001 <ts> 01 0012 0020 <32 key bytes> 00000001`.

---

## 6. The one-line `sspi` patch (the only blocker)

**Symptom (upstream sspi 0.21, against MIT KDC):** op 2 fails with

```
InvalidToken: ASN1 DER error: Expected Application number tag 25 but got: 26
```

**Cause:** `sspi`'s client decodes the decrypted AS-REP enc-part **strictly** as
`EncASRepPart` (ASN.1 `APPLICATION 25`):

```rust
// sspi-0.21.0/src/kerberos/client/extractors.rs  (fn extract_session_key_from_as_rep)
let enc_data = cipher.decrypt(&key, AS_REP_ENC, &as_rep.0.enc_part.0.cipher.0.0)?;
let enc_as_rep_part: EncAsRepPart = picky_asn1_der::from_bytes(&enc_data)?;   // <-- only accepts tag 25
Ok(enc_as_rep_part.0.key.0.key_value.0.to_vec().into())
```

MIT krb5 (1.20.1, the version real Kafka deployments use) tags the AS-REP
enc-part as `EncTGSRepPart` (`APPLICATION 26`). **RFC 4120 §5.4.2 explicitly
permits this and requires clients to accept either tag.** sspi does not — a
genuine interop bug, not a config issue.

**Fix (try tag 25, fall back to tag 26):**

```rust
let enc_data = cipher.decrypt(&key, AS_REP_ENC, &as_rep.0.enc_part.0.cipher.0.0)?;

// RFC 4120 5.4.2: AS-REP enc-part MAY be tagged EncTGSRepPart (APPLICATION 26);
// clients MUST accept either. MIT KDC emits tag 26.
let key_value = match picky_asn1_der::from_bytes::<EncAsRepPart>(&enc_data) {
    Ok(part) => part.0.key.0.key_value.0.to_vec(),
    Err(_) => {
        let part: EncTgsRepPart = picky_asn1_der::from_bytes(&enc_data)?;
        part.0.key.0.key_value.0.to_vec()
    }
};
Ok(key_value.into())
```

(`EncTgsRepPart` is already imported in that module — no new imports needed.)

**Verified:** with this patch applied (via `[patch.crates-io] sspi = { path = ... }`)
plus the §2 pbkdf2 bump, the spike runs end-to-end:

```
[1] keytab parsed: principal=kafka/localhost@CRABKA.TEST enctype=18 kvno=1 key_len=32
    OK: extracted 32-byte aes256 service key
[2] client initialize_security_context: status=ContinueNeeded, AP-REQ token = 747 bytes
    OK: client produced AP-REQ
[3] server accept_security_context: status=Ok, AP-REP token = 157 bytes
    recovered principal: inner="alice@crabka.test" account="alice" domain=Some("crabka.test")
    OK: source principal recovered and matches alice@CRABKA.TEST
    client consumed AP-REP: status=Ok
[4] client encrypt_message(WRAP_NO_ENCRYPT): 4 plaintext bytes -> 64 wrapped bytes
    server decrypt_message -> 4 bytes: [01, 00, 10, 00]
    OK: wrap/unwrap round-trip succeeded with confidentiality disabled
== ALL FOUR OPERATIONS SUCCEEDED -> GO ==
```

**Task 5 decision needed:** how to carry the patch. Options, in rough order of
preference:
1. **Upstream it** to `devolutions/sspi-rs` (tiny, RFC-justified) and pin a git
   rev until released. Lowest long-term maintenance.
2. **Vendor a minimal fork** under e.g. `crates/security/vendor/sspi` (~1.5 MB)
   referenced via `[patch.crates-io]`. Self-contained, no network dep, but bulky.
3. Local git fork + `[patch.crates-io]` git rev. Reproducible, external dep.

The spike intentionally does NOT commit the fork: the committed example is built
against upstream sspi so it compiles in CI, and it detects this exact error and
prints the GO-conditional message + a pointer to this doc, then exits 2.

---

## 7. Other behaviors worth carrying into downstream tasks

- **Pre-auth is mandatory** on client principals for sspi's client path (§3).
  The broker's own service principal does not need it (the broker only *accepts*).
- **`network_client` feature** is required for the client/initiate path
  (inter-broker auth, Task 9). The server/accept path (Task 6/8) does **not**
  touch the network — it only needs the keytab key — so the broker can accept
  GSSAPI logins even with no KDC connectivity at accept time.
- **Realm casing** is lowercased on principal recovery (§4d) — normalize in
  `auth_to_local`.
- **`SecurityStatus::ContinueNeeded` vs `Ok`**: with `MUTUAL_AUTH`, the client's
  first `initialize_security_context` returns `ContinueNeeded` and emits the
  AP-REQ; the server accept returns `Ok` and emits the AP-REP; feeding the AP-REP
  back to a second client `initialize_security_context` returns `Ok`. The server
  state machine (Task 6) keys off these statuses + output-token presence.
- **Replay cache**: sspi's `ServerProperties.authenticators_cache` already
  implements the RFC 4120 §3.2.3 authenticator replay cache in-process.

---

## 8. Files produced by this spike

- `crates/security/Cargo.toml` — adds `sspi = { version = "0.21", default-features = false, features = ["network_client", "ring"] }`
- root `Cargo.toml` — bumps `pbkdf2` to `=0.13.0-rc.10` (required for sspi 0.21 to resolve; §2)
- `crates/security/examples/gssapi_spike.rs` — the throwaway proof binary (+ minimal keytab parser)
- `crates/security/tests/fixtures/kdc/` — `Dockerfile`, `docker-compose.yml`, `krb5.conf`, `kdc.conf`, `setup.sh`, and the exported `kafka.keytab` / `alice.keytab`
- `docs/superpowers/specs/2026-05-28-gssapi-sspi-findings.md` — this doc

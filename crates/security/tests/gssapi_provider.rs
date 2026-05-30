//! KDC-backed contract test for the sspi-rs GSSAPI providers.
//!
//! This test requires the MIT KDC fixture and real network access to the KDC,
//! so it is `#[ignore]`d. Bring the fixture up first:
//!
//! ```text
//! cd crates/security/tests/fixtures/kdc && docker compose up --build -d
//! docker compose logs | grep -m1 KDC_READY   # wait until ready
//! ```
//!
//! Then run (the workspace forbids `unsafe`, so the env vars must be exported on
//! the command line — the test cannot set them itself):
//!
//! ```text
//! KRB5_CONFIG=crates/security/tests/fixtures/kdc/krb5.conf SSPI_KDC_URL=tcp://localhost:88 \
//!   cargo test -p crabka-security --test gssapi_provider -- --ignored
//! ```
//!
//! It drives the full GSSAPI loop against the fixture: `alice@CRABKA.TEST`
//! initiates *from a keytab* (`alice.keytab`, no password) to the
//! `kafka/localhost@CRABKA.TEST` service whose key lives in `kafka.keytab`,
//! asserts the recovered source principal, then round-trips a wrapped RFC 4752
//! security-layer message. This exercises the vendored sspi keytab-client-auth
//! path end-to-end against a real KDC.

use assert2::assert;
use crabka_security::gssapi::provider::{SspiAcceptor, SspiInitiator};
use crabka_security::gssapi::{AcceptStep, GssAcceptor, GssInitiator, InitStep};

const KEYTAB_PATH: &str = "tests/fixtures/kdc/kafka.keytab";
const SERVICE_NAME: &str = "kafka";
const TARGET_SPN: &str = "kafka/localhost";
const CLIENT_PRINCIPAL: &str = "alice@CRABKA.TEST";
const CLIENT_KEYTAB_PATH: &str = "tests/fixtures/kdc/alice.keytab";

#[test]
#[ignore = "requires the MIT KDC fixture (docker compose up) + exported KRB5_CONFIG/SSPI_KDC_URL"]
fn full_gssapi_handshake_and_wrap_roundtrip() {
    // Skip even under `--include-ignored` (the jvm-differential coverage job
    // runs ignored tests) unless a KDC is actually reachable. `SSPI_KDC_URL`
    // being exported is the signal that the docker compose fixture is up.
    let Ok(kdc_url) = std::env::var("SSPI_KDC_URL") else {
        eprintln!(
            "Skipping full_gssapi_handshake_and_wrap_roundtrip: set SSPI_KDC_URL \
             (and KRB5_CONFIG) and bring up the MIT KDC fixture under \
             crates/security/tests/fixtures/kdc to run."
        );
        return;
    };

    let mut acceptor = SspiAcceptor::new(KEYTAB_PATH, SERVICE_NAME).expect("build acceptor");
    let mut initiator =
        SspiInitiator::new(CLIENT_KEYTAB_PATH, CLIENT_PRINCIPAL, TARGET_SPN, &kdc_url)
            .expect("build initiator");

    // Drive the context-establishment loop: initiator produces a token, acceptor
    // consumes it, alternating until both sides report established.
    let mut server_token: Option<Vec<u8>> = None;
    let mut acceptor_done = false;
    let mut initiator_done = false;

    for _ in 0..8 {
        if !initiator_done {
            match initiator
                .step(server_token.as_deref())
                .expect("initiator step")
            {
                InitStep::Continue(token) => {
                    feed_acceptor(&mut acceptor, &token, &mut server_token, &mut acceptor_done);
                }
                InitStep::Established(token) => {
                    initiator_done = true;
                    if let Some(token) = token {
                        feed_acceptor(&mut acceptor, &token, &mut server_token, &mut acceptor_done);
                    }
                }
            }
        }

        if acceptor_done && initiator_done {
            break;
        }

        // If the acceptor produced an AP-REP but the initiator hasn't consumed
        // it yet, loop again so the initiator's mutual-auth second leg runs.
        if initiator_done && acceptor_done {
            break;
        }
        if initiator_done && server_token.is_none() {
            break;
        }
    }

    assert!(acceptor_done, "acceptor never reached Established");

    // The recovered principal: sspi lowercases the realm; compare case-insensitively.
    let principal = acceptor.src_principal().expect("src_principal");
    assert!(
        principal.to_ascii_lowercase() == CLIENT_PRINCIPAL.to_ascii_lowercase(),
        "recovered principal mismatch (got {principal})"
    );

    // RFC 4752 security-layer round-trip: acceptor wraps, initiator unwraps.
    let plaintext = [0x01u8, 0x00, 0x10, 0x00];
    let wrapped = acceptor.wrap(&plaintext, false).expect("acceptor wrap");
    let unwrapped = initiator.unwrap(&wrapped).expect("initiator unwrap");
    assert!(unwrapped == plaintext, "wrap/unwrap round-trip mismatch");
}

/// Feed a client token into the acceptor, recording any reply token and whether
/// the acceptor reached Established.
fn feed_acceptor(
    acceptor: &mut SspiAcceptor,
    token: &[u8],
    server_token: &mut Option<Vec<u8>>,
    acceptor_done: &mut bool,
) {
    match acceptor.accept(token).expect("acceptor accept") {
        AcceptStep::Continue(reply) => {
            *server_token = Some(reply);
        }
        AcceptStep::Established(reply) => {
            *acceptor_done = true;
            *server_token = reply;
        }
    }
}

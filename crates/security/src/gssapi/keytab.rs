//! MIT Kerberos keytab (version `0x0502`) parser.
//!
//! `sspi` does not ingest keytab files; its acceptor wants the raw
//! ticket-decryption key bytes ([`sspi::Secret<Vec<u8>>`]). This module parses
//! the on-disk MIT keytab format ourselves and extracts the service's long-term
//! key so [`crate::gssapi::provider::SspiAcceptor`] can build server
//! credentials.
//!
//! The format (big-endian throughout) is:
//!
//! ```text
//! file header : u16 magic (0x0502)
//! per entry   : i32 entry_size   ; length of the rest of the entry.
//!                                 ; NEGATIVE => hole; skip |entry_size| bytes.
//!               u16 num_components ; count EXCLUDES the realm
//!               counted realm     ; u16 len + bytes
//!               num_components × counted component
//!               u32 name_type
//!               u32 timestamp
//!               u8  kvno8
//!               keyblock          ; u16 enctype + u16 key_len + key bytes
//!               [u32 kvno32]      ; present iff >= 4 bytes remain in the entry;
//!                                 ; overrides kvno8.
//! ```
//!
//! Reference: MIT krb5 `lib/krb5/keytab/kt_file.c` and the documented
//! [keytab file format].
//!
//! [keytab file format]: https://web.mit.edu/kerberos/krb5-devel/doc/formats/keytab_file_format.html

/// aes256-cts-hmac-sha1-96. The only enctype Crabka's acceptor uses.
pub const ENCTYPE_AES256_CTS_HMAC_SHA1_96: u16 = 18;

/// A single key entry parsed from a keytab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeytabKey {
    /// Principal name components, excluding the realm (e.g. `["kafka", "host"]`).
    pub components: Vec<String>,
    /// The principal's realm (e.g. `"CRABKA.TEST"`).
    pub realm: String,
    /// Kerberos encryption type (18 = aes256-cts-hmac-sha1-96).
    pub enctype: u16,
    /// Key version number.
    pub kvno: u32,
    /// Raw long-term key bytes (32 bytes for aes256).
    pub key: Vec<u8>,
}

/// Errors produced while parsing a keytab or locating a service key.
#[derive(Debug, thiserror::Error)]
pub enum KeytabError {
    /// The file did not start with the expected `0x0502` magic.
    #[error("unexpected keytab magic 0x{0:04x}, expected 0x0502")]
    BadMagic(u16),
    /// The byte stream ended in the middle of a field (corrupt / truncated).
    #[error("keytab truncated: needed {needed} more bytes at offset {offset}")]
    Truncated {
        /// Offset at which the read was attempted.
        offset: usize,
        /// Number of bytes the read required.
        needed: usize,
    },
    /// No entry matched the requested service name / enctype.
    #[error("no key for service '{service}' with enctype {enctype} found in keytab")]
    NotFound {
        /// The first-component service name searched for.
        service: String,
        /// The enctype searched for.
        enctype: u16,
    },
}

/// Cursor over a byte slice with bounds-checked big-endian reads.
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], KeytabError> {
        if self.remaining() < n {
            return Err(KeytabError::Truncated {
                offset: self.pos,
                needed: n,
            });
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, KeytabError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, KeytabError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn i32(&mut self) -> Result<i32, KeytabError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u32(&mut self) -> Result<u32, KeytabError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read a `u16`-length-prefixed string (lossy UTF-8 decode).
    fn counted_str(&mut self) -> Result<String, KeytabError> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }
}

/// Parse every key entry in a keytab, skipping holes.
///
/// # Errors
///
/// Returns [`KeytabError::BadMagic`] if the file header is wrong, or
/// [`KeytabError::Truncated`] if any entry runs past the end of the buffer.
pub fn parse_keytab(bytes: &[u8]) -> Result<Vec<KeytabKey>, KeytabError> {
    let mut reader = Reader::new(bytes);

    let magic = reader.u16()?;
    if magic != 0x0502 {
        return Err(KeytabError::BadMagic(magic));
    }

    let mut entries = Vec::new();
    while !reader.at_end() {
        // Some keytabs leave trailing padding shorter than a size field; treat a
        // truncated size read at this boundary as end-of-file rather than error.
        let entry_size = match reader.i32() {
            Ok(size) => size,
            Err(KeytabError::Truncated { .. }) => break,
            Err(e) => return Err(e),
        };

        if entry_size <= 0 {
            // Hole: skip |entry_size| bytes of freed space.
            let hole = entry_size.unsigned_abs() as usize;
            reader.take(hole)?;
            continue;
        }

        // `entry_size > 0` is guaranteed above, so the conversion is exact.
        let entry_size = usize::try_from(entry_size).expect("entry_size is positive");
        let entry_start = reader.pos;
        let entry_end = entry_start + entry_size;
        if entry_end > bytes.len() {
            return Err(KeytabError::Truncated {
                offset: entry_start,
                needed: entry_size,
            });
        }

        let num_components = reader.u16()? as usize;
        let realm = reader.counted_str()?;
        let mut components = Vec::with_capacity(num_components);
        for _ in 0..num_components {
            components.push(reader.counted_str()?);
        }
        let _name_type = reader.u32()?;
        let _timestamp = reader.u32()?;
        let kvno8 = u32::from(reader.u8()?);

        // keyblock: u16 enctype + u16 key_len + key bytes.
        let enctype = reader.u16()?;
        let key_len = reader.u16()? as usize;
        let key = reader.take(key_len)?.to_vec();

        // Optional 32-bit kvno: present iff >= 4 bytes remain in this entry.
        let kvno = if entry_end.saturating_sub(reader.pos) >= 4 {
            reader.u32()?
        } else {
            kvno8
        };

        // Advance to the entry boundary in case of trailing/unknown bytes.
        reader.pos = entry_end;

        entries.push(KeytabKey {
            components,
            realm,
            enctype,
            kvno,
            key,
        });
    }

    Ok(entries)
}

/// Find the key for the principal whose FIRST component equals `service_name`,
/// choosing the highest-kvno entry with the given `enctype`.
///
/// # Errors
///
/// Returns the same parse errors as [`parse_keytab`], or
/// [`KeytabError::NotFound`] when no matching entry exists.
pub fn load_service_key(
    keytab_bytes: &[u8],
    service_name: &str,
    enctype: u16,
) -> Result<KeytabKey, KeytabError> {
    let entries = parse_keytab(keytab_bytes)?;
    entries
        .into_iter()
        .filter(|e| e.enctype == enctype && e.components.first().is_some_and(|c| c == service_name))
        .max_by_key(|e| e.kvno)
        .ok_or_else(|| KeytabError::NotFound {
            service: service_name.to_owned(),
            enctype,
        })
}

/// Find every distinct service principal whose FIRST component equals
/// `service_name`, returning the highest-kvno key for each (one [`KeytabKey`]
/// per distinct principal-name component list).
///
/// A keytab routinely holds keys for several host SPNs of the same service
/// (e.g. `kafka/localhost` and `kafka/host.docker.internal`). The GSSAPI
/// acceptor needs all of them so it can validate an incoming ticket against
/// whichever SPN the client actually named — matching the JVM broker, which
/// keys off the whole keytab rather than one pinned hostname.
///
/// Results are ordered by their component lists for determinism. Empty when no
/// entry matches.
///
/// # Errors
///
/// Returns the same parse errors as [`parse_keytab`].
pub fn load_service_keys(
    keytab_bytes: &[u8],
    service_name: &str,
    enctype: u16,
) -> Result<Vec<KeytabKey>, KeytabError> {
    let entries = parse_keytab(keytab_bytes)?;
    let mut best: std::collections::BTreeMap<Vec<String>, KeytabKey> =
        std::collections::BTreeMap::new();
    for e in entries {
        if e.enctype != enctype || e.components.first().is_none_or(|c| c != service_name) {
            continue;
        }
        match best.get(&e.components) {
            Some(existing) if existing.kvno >= e.kvno => {}
            _ => {
                best.insert(e.components.clone(), e);
            }
        }
    }
    Ok(best.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    /// Fields needed to synthesize a keytab entry body in tests.
    struct EntrySpec<'a> {
        components: &'a [&'a str],
        realm: &'a str,
        kvno8: u8,
        enctype: u16,
        key: &'a [u8],
        /// Adds a trailing 32-bit kvno (overrides `kvno8`) when `Some`.
        kvno32: Option<u32>,
    }

    /// Build one keytab entry body (everything after the i32 size field).
    fn entry_body(spec: &EntrySpec<'_>) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(u16::try_from(spec.components.len()).unwrap()).to_be_bytes());
        b.extend_from_slice(&(u16::try_from(spec.realm.len()).unwrap()).to_be_bytes());
        b.extend_from_slice(spec.realm.as_bytes());
        for c in spec.components {
            b.extend_from_slice(&(u16::try_from(c.len()).unwrap()).to_be_bytes());
            b.extend_from_slice(c.as_bytes());
        }
        b.extend_from_slice(&1u32.to_be_bytes()); // name_type = NT_PRINCIPAL
        b.extend_from_slice(&0x1234_5678u32.to_be_bytes()); // timestamp
        b.push(spec.kvno8);
        b.extend_from_slice(&spec.enctype.to_be_bytes());
        b.extend_from_slice(&(u16::try_from(spec.key.len()).unwrap()).to_be_bytes());
        b.extend_from_slice(spec.key);
        if let Some(kvno32) = spec.kvno32 {
            b.extend_from_slice(&kvno32.to_be_bytes());
        }
        b
    }

    fn keytab(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut out = vec![0x05, 0x02];
        for e in entries {
            out.extend_from_slice(&(i32::try_from(e.len()).unwrap()).to_be_bytes());
            out.extend_from_slice(e);
        }
        out
    }

    #[test]
    fn parses_single_aes256_entry() {
        // Mirrors the fixture hexdump documented in the task:
        // 0502 0000 0052 0002 000b "CRABKA.TEST" 0005 "kafka" 0009 "localhost"
        // 00000001 <ts> 01 0012 0020 <32 key bytes> 00000001
        let key: Vec<u8> = (0u8..32).collect();
        let body = entry_body(&EntrySpec {
            components: &["kafka", "localhost"],
            realm: "CRABKA.TEST",
            kvno8: 1,
            enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
            key: &key,
            kvno32: Some(1),
        });
        let kt = keytab(&[body]);

        let entries = parse_keytab(&kt).expect("parse");
        assert!(
            entries
                == vec![KeytabKey {
                    components: vec!["kafka".to_string(), "localhost".to_string()],
                    realm: "CRABKA.TEST".to_string(),
                    enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                    kvno: 1,
                    key: key.clone(),
                }]
        );

        let found = load_service_key(&kt, "kafka", ENCTYPE_AES256_CTS_HMAC_SHA1_96).expect("found");
        assert!(found.key == key);
    }

    #[test]
    fn highest_kvno_and_enctype_filter_win() {
        // Same principal, three entries: an old aes256 (kvno 1), a newer aes256
        // (kvno 5), and a same-kvno aes128 (enctype 17) decoy.
        let key_v1 = vec![0xAAu8; 32];
        let key_v5 = vec![0xBBu8; 32];
        let key_aes128 = vec![0xCCu8; 16];
        let kt = keytab(&[
            entry_body(&EntrySpec {
                components: &["kafka", "host"],
                realm: "R",
                kvno8: 1,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &key_v1,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "host"],
                realm: "R",
                kvno8: 17,
                enctype: 17,
                key: &key_aes128,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "host"],
                realm: "R",
                kvno8: 5,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &key_v5,
                kvno32: None,
            }),
        ]);

        let found = load_service_key(&kt, "kafka", ENCTYPE_AES256_CTS_HMAC_SHA1_96).expect("found");
        assert!(found.kvno == 5);
        assert!(found.key == key_v5);

        // Wrong service name -> NotFound.
        let err = load_service_key(&kt, "http", ENCTYPE_AES256_CTS_HMAC_SHA1_96).unwrap_err();
        assert!(matches!(err, KeytabError::NotFound { .. }));
    }

    #[test]
    fn load_service_keys_dedups_per_spn_and_filters_enctype() {
        // Two distinct SPNs of the same service, the first with two kvnos, plus
        // an aes128 decoy and a different-service entry that must be excluded.
        let loc_v1 = vec![0x11u8; 32];
        let loc_v3 = vec![0x33u8; 32];
        let dock = vec![0x44u8; 32];
        let loc_aes128 = vec![0x99u8; 16];
        let http = vec![0x77u8; 32];
        let kt = keytab(&[
            entry_body(&EntrySpec {
                components: &["kafka", "localhost"],
                realm: "R",
                kvno8: 1,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &loc_v1,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "localhost"],
                realm: "R",
                kvno8: 3,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &loc_v3,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "localhost"],
                realm: "R",
                kvno8: 9,
                enctype: 17,
                key: &loc_aes128,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "host.docker.internal"],
                realm: "R",
                kvno8: 1,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &dock,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["http", "localhost"],
                realm: "R",
                kvno8: 1,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &http,
                kvno32: None,
            }),
        ]);

        let keys = load_service_keys(&kt, "kafka", ENCTYPE_AES256_CTS_HMAC_SHA1_96).expect("load");
        // One key per distinct SPN, ordered by component list. Highest kvno
        // wins for the duplicated SPN; aes128 decoy excluded.
        assert!(
            keys == vec![
                KeytabKey {
                    components: vec!["kafka".to_string(), "host.docker.internal".to_string()],
                    realm: "R".to_string(),
                    enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                    kvno: 1,
                    key: dock.clone(),
                },
                KeytabKey {
                    components: vec!["kafka".to_string(), "localhost".to_string()],
                    realm: "R".to_string(),
                    enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                    kvno: 3,
                    key: loc_v3.clone(),
                },
            ]
        );
    }

    #[test]
    fn load_service_keys_keeps_first_key_when_duplicate_kvno_ties() {
        let first = vec![0x11u8; 32];
        let second = vec![0x22u8; 32];
        let kt = keytab(&[
            entry_body(&EntrySpec {
                components: &["kafka", "localhost"],
                realm: "R",
                kvno8: 7,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &first,
                kvno32: None,
            }),
            entry_body(&EntrySpec {
                components: &["kafka", "localhost"],
                realm: "R",
                kvno8: 7,
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                key: &second,
                kvno32: None,
            }),
        ]);

        let keys = load_service_keys(&kt, "kafka", ENCTYPE_AES256_CTS_HMAC_SHA1_96).expect("load");
        assert!(
            keys == vec![KeytabKey {
                components: vec!["kafka".to_string(), "localhost".to_string()],
                realm: "R".to_string(),
                enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
                kvno: 7,
                key: first.clone(),
            }]
        );
    }

    #[test]
    fn load_service_keys_empty_when_no_match() {
        let key = vec![0x42u8; 32];
        let kt = keytab(&[entry_body(&EntrySpec {
            components: &["kafka", "localhost"],
            realm: "R",
            kvno8: 1,
            enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
            key: &key,
            kvno32: None,
        })]);
        let keys = load_service_keys(&kt, "http", ENCTYPE_AES256_CTS_HMAC_SHA1_96).expect("load");
        assert!(keys.is_empty());
    }

    #[test]
    fn holes_are_skipped() {
        let key = vec![0x42u8; 32];
        let good = entry_body(&EntrySpec {
            components: &["kafka", "host"],
            realm: "R",
            kvno8: 1,
            enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
            key: &key,
            kvno32: None,
        });

        // Header, then a hole (negative size => skip 4 bytes of junk), then a
        // real entry. Build manually to inject the negative size field.
        let mut kt = vec![0x05, 0x02];
        kt.extend_from_slice(&(-4i32).to_be_bytes()); // hole header
        kt.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // 4 freed bytes
        kt.extend_from_slice(&(i32::try_from(good.len()).unwrap()).to_be_bytes());
        kt.extend_from_slice(&good);

        let entries = parse_keytab(&kt).expect("parse");
        assert!(entries.len() == 1, "hole must not produce an entry");
        assert!(entries[0].key == key);
    }

    #[test]
    fn parse_accepts_trailing_padding_shorter_than_size_field() {
        let key = vec![0x42u8; 32];
        let body = entry_body(&EntrySpec {
            components: &["kafka", "host"],
            realm: "R",
            kvno8: 1,
            enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
            key: &key,
            kvno32: None,
        });
        let mut kt = keytab(&[body]);
        kt.extend_from_slice(&[0x00, 0x00, 0x00]);

        let entries = parse_keytab(&kt).expect("parse");
        assert!(entries.len() == 1);
        assert!(entries[0].key == key);
    }

    #[test]
    fn bad_magic_rejected() {
        let err = parse_keytab(&[0x05, 0x01, 0x00, 0x00]).unwrap_err();
        assert!(matches!(err, KeytabError::BadMagic(0x0501)));
    }

    #[test]
    fn truncated_entry_rejected() {
        let key = vec![0x00u8; 32];
        let body = entry_body(&EntrySpec {
            components: &["kafka", "host"],
            realm: "R",
            kvno8: 1,
            enctype: ENCTYPE_AES256_CTS_HMAC_SHA1_96,
            key: &key,
            kvno32: None,
        });
        let mut kt = keytab(&[body]);
        kt.truncate(kt.len() - 10); // chop the tail of the entry
        assert!(matches!(
            parse_keytab(&kt),
            Err(KeytabError::Truncated { .. })
        ));
    }
}

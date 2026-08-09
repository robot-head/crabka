//! Dispatch enum over the generated KIP-631 record types, keyed by the `KRaft`
//! metadata record apiKey. That apiKey namespace is distinct from the RPC
//! apiKeys. The enum encodes and decodes through the value envelope. Unknown
//! apiKeys decode to `Unknown`, so a forward-compatible reader can always read
//! the record.
//!
//! Multi-version records (`RegisterBroker`, `Partition`,
//! `BrokerRegistrationChange`, …) must re-encode at the same apiVersion they
//! were decoded at for a faithful byte round-trip. So
//! [`KraftMetadataRecord::decode_value`] returns the decoded apiVersion
//! alongside the record, and [`KraftMetadataRecord::encode_value`] takes the
//! target apiVersion as an argument.

use bytes::{Bytes, BytesMut};

use crate::{
    Decode, Encode, ProtocolError,
    owned::{
        access_control_entry_record::AccessControlEntryRecord,
        begin_transaction_record::BeginTransactionRecord,
        broker_registration_change_record::BrokerRegistrationChangeRecord,
        client_quota_record::ClientQuotaRecord, config_record::ConfigRecord,
        delegation_token_record::DelegationTokenRecord,
        end_transaction_record::EndTransactionRecord, feature_level_record::FeatureLevelRecord,
        no_op_record::NoOpRecord, partition_record::PartitionRecord,
        producer_ids_record::ProducerIdsRecord, register_broker_record::RegisterBrokerRecord,
        register_controller_record::RegisterControllerRecord,
        remove_access_control_entry_record::RemoveAccessControlEntryRecord,
        remove_delegation_token_record::RemoveDelegationTokenRecord,
        remove_topic_record::RemoveTopicRecord,
        remove_user_scram_credential_record::RemoveUserScramCredentialRecord,
        topic_record::TopicRecord, unregister_broker_record::UnregisterBrokerRecord,
        user_scram_credential_record::UserScramCredentialRecord,
    },
    records::metadata::envelope::{decode_value_header, encode_value},
};

/// A single `KRaft` metadata record (the value of one Kafka `Record`).
#[derive(Debug, Clone, PartialEq)]
pub enum KraftMetadataRecord {
    // apiKeys are Kafka's real (non-sequential) metadata-record keys; `api_key()`
    // is the authoritative mapping and these comments mirror it.
    RegisterBroker(RegisterBrokerRecord),           // apiKey 0
    UnregisterBroker(UnregisterBrokerRecord),       // apiKey 1
    Topic(TopicRecord),                             // apiKey 2
    Partition(PartitionRecord),                     // apiKey 3
    Config(ConfigRecord),                           // apiKey 4
    RemoveTopic(RemoveTopicRecord),                 // apiKey 9
    DelegationToken(DelegationTokenRecord),         // apiKey 10
    UserScramCredential(UserScramCredentialRecord), // apiKey 11
    FeatureLevel(FeatureLevelRecord),               // apiKey 12
    ClientQuota(ClientQuotaRecord),                 // apiKey 14
    ProducerIds(ProducerIdsRecord),                 // apiKey 15
    BrokerRegistrationChange(BrokerRegistrationChangeRecord), // apiKey 17
    AccessControlEntry(AccessControlEntryRecord),   // apiKey 18
    RemoveAccessControlEntry(RemoveAccessControlEntryRecord), // apiKey 19
    NoOp(NoOpRecord),                               // apiKey 20
    RemoveUserScramCredential(RemoveUserScramCredentialRecord), // apiKey 22
    BeginTransaction(BeginTransactionRecord),       // apiKey 23
    EndTransaction(EndTransactionRecord),           // apiKey 24
    RemoveDelegationToken(RemoveDelegationTokenRecord), // apiKey 26
    RegisterController(RegisterControllerRecord),   // apiKey 27
    /// A record this build does not model. Body is the post-envelope bytes.
    Unknown {
        api_key: u32,
        api_version: u32,
        body: Bytes,
    },
}

/// Widens a record apiVersion to the envelope's unsigned representation.
/// Negative versions have no meaning for metadata records, so they clamp to 0.
fn api_version_to_u32(version: i16) -> u32 {
    u32::try_from(version).unwrap_or(0)
}

/// Narrows the envelope's apiVersion to the `i16` the generated codecs take.
///
/// # Errors
/// Returns [`ProtocolError::SchemaMismatch`] if the declared version does not
/// fit in `i16`. No real metadata record version is that large.
fn api_version_to_i16(version: u32) -> Result<i16, ProtocolError> {
    i16::try_from(version).map_err(|_| ProtocolError::SchemaMismatch("metadata record apiVersion"))
}

impl KraftMetadataRecord {
    /// The fixed metadata-record apiKey this variant encodes as. The apiVersion
    /// is a runtime value that the envelope carries, and it is not part of this
    /// mapping.
    #[must_use]
    pub fn api_key(&self) -> u32 {
        match self {
            // apiKeys are Kafka's actual metadata-record keys (NOT sequential):
            // see metadata/src/main/resources/common/metadata/*.json.
            Self::RegisterBroker(_) => 0,
            Self::Topic(_) => 2,
            Self::Partition(_) => 3,
            Self::RemoveTopic(_) => 9,
            Self::FeatureLevel(_) => 12,
            Self::BrokerRegistrationChange(_) => 17,
            Self::NoOp(_) => 20,
            Self::BeginTransaction(_) => 23,
            Self::EndTransaction(_) => 24,
            Self::RegisterController(_) => 27,
            Self::UnregisterBroker(_) => 1,
            Self::Config(_) => 4,
            Self::DelegationToken(_) => 10,
            Self::UserScramCredential(_) => 11,
            Self::ClientQuota(_) => 14,
            Self::ProducerIds(_) => 15,
            Self::AccessControlEntry(_) => 18,
            Self::RemoveAccessControlEntry(_) => 19,
            Self::RemoveUserScramCredential(_) => 22,
            Self::RemoveDelegationToken(_) => 26,
            Self::Unknown { api_key, .. } => *api_key,
        }
    }

    /// Encodes this record to its value bytes, the envelope plus the body, at
    /// `api_version`.
    ///
    /// For a faithful round-trip, pass the apiVersion returned by
    /// [`Self::decode_value`]. For freshly built records, pass the version you
    /// intend to write. `FeatureLevelRecord` is always apiVersion 0.
    ///
    /// # Errors
    /// Propagates a [`ProtocolError`] from the underlying message encoder.
    pub fn encode_value(&self, api_version: i16) -> Result<Bytes, ProtocolError> {
        let api_key = self.api_key();
        let v = api_version;
        let mut body = BytesMut::new();
        match self {
            Self::RegisterBroker(r) => r.encode(&mut body, v)?,
            Self::Topic(r) => r.encode(&mut body, v)?,
            Self::Partition(r) => r.encode(&mut body, v)?,
            Self::RemoveTopic(r) => r.encode(&mut body, v)?,
            Self::BeginTransaction(r) => r.encode(&mut body, v)?,
            Self::EndTransaction(r) => r.encode(&mut body, v)?,
            Self::NoOp(r) => r.encode(&mut body, v)?,
            Self::RegisterController(r) => r.encode(&mut body, v)?,
            Self::BrokerRegistrationChange(r) => r.encode(&mut body, v)?,
            Self::FeatureLevel(r) => r.encode(&mut body, v)?,
            Self::UnregisterBroker(r) => r.encode(&mut body, v)?,
            Self::Config(r) => r.encode(&mut body, v)?,
            Self::DelegationToken(r) => r.encode(&mut body, v)?,
            Self::UserScramCredential(r) => r.encode(&mut body, v)?,
            Self::ClientQuota(r) => r.encode(&mut body, v)?,
            Self::ProducerIds(r) => r.encode(&mut body, v)?,
            Self::AccessControlEntry(r) => r.encode(&mut body, v)?,
            Self::RemoveAccessControlEntry(r) => r.encode(&mut body, v)?,
            Self::RemoveUserScramCredential(r) => r.encode(&mut body, v)?,
            Self::RemoveDelegationToken(r) => r.encode(&mut body, v)?,
            Self::Unknown { body: raw, .. } => {
                return Ok(encode_value(api_key, api_version_to_u32(api_version), raw));
            }
        }
        Ok(encode_value(
            api_key,
            api_version_to_u32(api_version),
            &body,
        ))
    }

    /// Decodes one record from its value bytes.
    ///
    /// Returns the record and the apiVersion the envelope declared. The caller
    /// needs that apiVersion to re-encode the record byte-identically.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the envelope or body cannot be decoded.
    pub fn decode_value(value: &[u8]) -> Result<(Self, i16), ProtocolError> {
        let mut cur: &[u8] = value;
        let hdr = decode_value_header(&mut cur)
            .map_err(|_| ProtocolError::SchemaMismatch("metadata record envelope"))?;
        let v = api_version_to_i16(hdr.api_version)?;
        let rec = match hdr.api_key {
            0 => Self::RegisterBroker(RegisterBrokerRecord::decode(&mut cur, v)?),
            2 => Self::Topic(TopicRecord::decode(&mut cur, v)?),
            3 => Self::Partition(PartitionRecord::decode(&mut cur, v)?),
            9 => Self::RemoveTopic(RemoveTopicRecord::decode(&mut cur, v)?),
            12 => Self::FeatureLevel(FeatureLevelRecord::decode(&mut cur, v)?),
            17 => {
                Self::BrokerRegistrationChange(BrokerRegistrationChangeRecord::decode(&mut cur, v)?)
            }
            20 => Self::NoOp(NoOpRecord::decode(&mut cur, v)?),
            23 => Self::BeginTransaction(BeginTransactionRecord::decode(&mut cur, v)?),
            24 => Self::EndTransaction(EndTransactionRecord::decode(&mut cur, v)?),
            27 => Self::RegisterController(RegisterControllerRecord::decode(&mut cur, v)?),
            1 => Self::UnregisterBroker(UnregisterBrokerRecord::decode(&mut cur, v)?),
            4 => Self::Config(ConfigRecord::decode(&mut cur, v)?),
            10 => Self::DelegationToken(DelegationTokenRecord::decode(&mut cur, v)?),
            11 => Self::UserScramCredential(UserScramCredentialRecord::decode(&mut cur, v)?),
            14 => Self::ClientQuota(ClientQuotaRecord::decode(&mut cur, v)?),
            15 => Self::ProducerIds(ProducerIdsRecord::decode(&mut cur, v)?),
            18 => Self::AccessControlEntry(AccessControlEntryRecord::decode(&mut cur, v)?),
            19 => {
                Self::RemoveAccessControlEntry(RemoveAccessControlEntryRecord::decode(&mut cur, v)?)
            }
            22 => Self::RemoveUserScramCredential(RemoveUserScramCredentialRecord::decode(
                &mut cur, v,
            )?),
            26 => Self::RemoveDelegationToken(RemoveDelegationTokenRecord::decode(&mut cur, v)?),
            other => {
                // Unknown records keep their raw post-envelope bytes verbatim,
                // so trailing bytes are intentional here (not checked below).
                return Ok((
                    Self::Unknown {
                        api_key: other,
                        api_version: hdr.api_version,
                        body: Bytes::copy_from_slice(cur),
                    },
                    v,
                ));
            }
        };
        // A modeled record body must consume the whole value. Trailing bytes mean
        // the record carries fields this build does not model — fail loudly so a
        // round-trip would not silently re-encode shorter (mirrors the
        // trailing-byte rejection in the RecordBatch/Record layers).
        if !cur.is_empty() {
            return Err(ProtocolError::SchemaMismatch(
                "trailing bytes after metadata record body",
            ));
        }
        Ok((rec, v))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn feature_level_record_value_roundtrips_through_dispatch() {
        let rec = KraftMetadataRecord::FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_string(),
            feature_level: 25,
            ..Default::default()
        });
        let value = rec.encode_value(0).expect("encode");
        let (decoded, ver) = KraftMetadataRecord::decode_value(&value).expect("decode");
        check!(decoded == rec);
        check!(ver == 0);
        // Re-encode at the decoded version is byte-identical.
        check!(decoded.encode_value(ver).expect("re-encode") == value);
    }

    #[test]
    fn unknown_api_key_decodes_to_unknown_arm() {
        use crate::records::metadata::envelope::encode_value;
        // apiKey 99 is not modeled.
        let value = encode_value(99, 0, &[0xAB, 0xCD]);
        let (decoded, ver) = KraftMetadataRecord::decode_value(&value).expect("decode");
        assert2::assert!(ver == 0);
        let expected = KraftMetadataRecord::Unknown {
            api_key: 99,
            api_version: 0,
            body: Bytes::from_static(&[0xAB, 0xCD]),
        };
        assert2::assert!(decoded == expected);
        // Unknown re-encodes byte-identically too.
        assert2::assert!(decoded.encode_value(ver).expect("re-encode") == value);
    }

    #[test]
    fn config_record_value_roundtrips() {
        use crate::owned::config_record::ConfigRecord;
        let rec = KraftMetadataRecord::Config(ConfigRecord::default());
        let (decoded, ver) =
            KraftMetadataRecord::decode_value(&rec.encode_value(0).unwrap()).unwrap();
        assert2::assert!(decoded.encode_value(ver).unwrap() == rec.encode_value(0).unwrap());
        assert2::assert!(decoded.api_key() == 4);
    }

    #[test]
    fn all_new_records_have_correct_api_keys() {
        use crate::owned::{
            access_control_entry_record::AccessControlEntryRecord,
            client_quota_record::ClientQuotaRecord, config_record::ConfigRecord,
            delegation_token_record::DelegationTokenRecord, producer_ids_record::ProducerIdsRecord,
            remove_access_control_entry_record::RemoveAccessControlEntryRecord,
            remove_delegation_token_record::RemoveDelegationTokenRecord,
            remove_user_scram_credential_record::RemoveUserScramCredentialRecord,
            unregister_broker_record::UnregisterBrokerRecord,
            user_scram_credential_record::UserScramCredentialRecord,
        };
        let cases = [
            (
                KraftMetadataRecord::UnregisterBroker(UnregisterBrokerRecord::default()),
                1,
            ),
            (KraftMetadataRecord::Config(ConfigRecord::default()), 4),
            (
                KraftMetadataRecord::DelegationToken(DelegationTokenRecord::default()),
                10,
            ),
            (
                KraftMetadataRecord::UserScramCredential(UserScramCredentialRecord::default()),
                11,
            ),
            (
                KraftMetadataRecord::ClientQuota(ClientQuotaRecord::default()),
                14,
            ),
            (
                KraftMetadataRecord::ProducerIds(ProducerIdsRecord::default()),
                15,
            ),
            (
                KraftMetadataRecord::AccessControlEntry(AccessControlEntryRecord::default()),
                18,
            ),
            (
                KraftMetadataRecord::RemoveAccessControlEntry(
                    RemoveAccessControlEntryRecord::default(),
                ),
                19,
            ),
            (
                KraftMetadataRecord::RemoveUserScramCredential(
                    RemoveUserScramCredentialRecord::default(),
                ),
                22,
            ),
            (
                KraftMetadataRecord::RemoveDelegationToken(RemoveDelegationTokenRecord::default()),
                26,
            ),
        ];
        for (rec, want) in cases {
            assert2::assert!(rec.api_key() == want);
        }
    }
}

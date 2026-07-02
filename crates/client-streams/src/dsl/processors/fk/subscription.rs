//! KIP-213 subscription + response wrappers (JVM
//! `org.apache.kafka.streams.kstream.internals.foreignkeyjoin`).
//!
//! `SubscriptionWrapper`  : `version1 | (isHashNull<<7)` ‖ `instruction(1)` ‖ `[hash:16]` ‖ `pk…` ‖ `primaryPartition:4BE`
//! `SubscriptionResponse` : `version0 | (isHashNull<<7)` ‖ `[hash:16]` ‖ `[foreignValue…]`
//! Byte-exact vs the `--fkjoin` capture.
//! Consumed by the FK-join subscription store + the five FK-join processors.
use bytes::{BufMut, Bytes, BytesMut};

const SUB_VERSION: u8 = 1;
const RESP_VERSION: u8 = 0;
const HASH_NULL_FLAG: u8 = 0x80;
const HASH_LEN: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Instruction {
    PropagateOnlyIfFkValAvailable,
    PropagateNullIfNoFkValAvailable,
    DeleteKeyAndPropagate,
    DeleteKeyNoPropagate,
}

impl Instruction {
    pub(crate) fn to_byte(self) -> u8 {
        match self {
            Instruction::DeleteKeyNoPropagate => 0,
            Instruction::DeleteKeyAndPropagate => 1,
            Instruction::PropagateNullIfNoFkValAvailable => 2,
            Instruction::PropagateOnlyIfFkValAvailable => 3,
        }
    }
    pub(crate) fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0 => Instruction::DeleteKeyNoPropagate,
            1 => Instruction::DeleteKeyAndPropagate,
            2 => Instruction::PropagateNullIfNoFkValAvailable,
            3 => Instruction::PropagateOnlyIfFkValAvailable,
            _ => return None,
        })
    }
    /// JVM enum name — used only to assert ordinal parity against the capture.
    #[cfg(test)]
    pub(crate) fn name(self) -> &'static str {
        match self {
            Instruction::DeleteKeyNoPropagate => "DELETE_KEY_NO_PROPAGATE",
            Instruction::DeleteKeyAndPropagate => "DELETE_KEY_AND_PROPAGATE",
            Instruction::PropagateNullIfNoFkValAvailable => "PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE",
            Instruction::PropagateOnlyIfFkValAvailable => "PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionWrapper {
    pub instruction: Instruction,
    pub hash: Option<Vec<u8>>,
    pub primary_key: Bytes,
    pub primary_partition: i32,
}

impl SubscriptionWrapper {
    pub(crate) fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        let v0 = if self.hash.is_none() {
            SUB_VERSION | HASH_NULL_FLAG
        } else {
            SUB_VERSION
        };
        b.put_u8(v0);
        b.put_u8(self.instruction.to_byte());
        if let Some(h) = &self.hash {
            debug_assert_eq!(h.len(), HASH_LEN);
            b.extend_from_slice(h);
        }
        b.extend_from_slice(&self.primary_key);
        b.put_i32(self.primary_partition);
        b.freeze()
    }
    pub(crate) fn deserialize(bytes: &[u8]) -> Self {
        let is_hash_null = bytes[0] & HASH_NULL_FLAG != 0;
        let instruction = Instruction::from_byte(bytes[1]).expect("valid instruction");
        let mut i = 2;
        let hash = if is_hash_null {
            None
        } else {
            let h = bytes[i..i + HASH_LEN].to_vec();
            i += HASH_LEN;
            Some(h)
        };
        let pk_end = bytes.len() - 4;
        let primary_key = Bytes::copy_from_slice(&bytes[i..pk_end]);
        let primary_partition = i32::from_be_bytes(bytes[pk_end..].try_into().expect("4 bytes"));
        Self {
            instruction,
            hash,
            primary_key,
            primary_partition,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionResponseWrapper {
    pub hash: Option<Vec<u8>>,
    pub foreign_value: Option<Bytes>,
}

impl SubscriptionResponseWrapper {
    pub(crate) fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        let v0 = if self.hash.is_none() {
            RESP_VERSION | HASH_NULL_FLAG
        } else {
            RESP_VERSION
        };
        b.put_u8(v0);
        if let Some(h) = &self.hash {
            debug_assert_eq!(h.len(), HASH_LEN);
            b.extend_from_slice(h);
        }
        if let Some(fv) = &self.foreign_value {
            b.extend_from_slice(fv);
        }
        b.freeze()
    }
    pub(crate) fn deserialize(bytes: &[u8]) -> Self {
        let is_hash_null = bytes[0] & HASH_NULL_FLAG != 0;
        let mut i = 1;
        let hash = if is_hash_null {
            None
        } else {
            let h = bytes[i..i + HASH_LEN].to_vec();
            i += HASH_LEN;
            Some(h)
        };
        let foreign_value = if i < bytes.len() {
            Some(Bytes::copy_from_slice(&bytes[i..]))
        } else {
            None
        };
        Self {
            hash,
            foreign_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn instruction_ordinals_match_capture() {
        let v = behavior();
        for e in v["instruction_ordinals"].as_array().unwrap() {
            let name = e["name"].as_str().unwrap();
            let byte = u8::try_from(e["byte"].as_u64().unwrap()).unwrap();
            assert_eq!(Instruction::from_byte(byte).unwrap().name(), name);
            assert_eq!(Instruction::from_byte(byte).unwrap().to_byte(), byte);
        }
    }

    #[test]
    fn subscription_wrapper_matches_capture() {
        let v = behavior();
        for e in v["subscription_wrapper"].as_array().unwrap() {
            let instr = Instruction::from_byte(
                u8::try_from(e["instruction_byte"].as_u64().unwrap()).unwrap(),
            )
            .unwrap();
            let hash = e["hash_hex"].as_str().map(hex);
            let pk = e["pk"].as_str().unwrap().as_bytes();
            let pp = i32::try_from(e["primary_partition"].as_i64().unwrap()).unwrap();
            let w = SubscriptionWrapper {
                instruction: instr,
                hash: hash.clone(),
                primary_key: Bytes::copy_from_slice(pk),
                primary_partition: pp,
            };
            assert_eq!(
                w.serialize(),
                Bytes::from(hex(e["bytes_hex"].as_str().unwrap())),
                "subscription wrapper bytes mismatch: {e}"
            );
            assert_eq!(SubscriptionWrapper::deserialize(&w.serialize()), w);
        }
    }

    #[test]
    fn response_wrapper_matches_capture() {
        let v = behavior();
        for e in v["subscription_response_wrapper"].as_array().unwrap() {
            let hash = e["hash_hex"].as_str().map(hex);
            let fv = e["foreign_value_hex"].as_str().map(|s| Bytes::from(hex(s)));
            let w = SubscriptionResponseWrapper {
                hash: hash.clone(),
                foreign_value: fv.clone(),
            };
            assert_eq!(
                w.serialize(),
                Bytes::from(hex(e["bytes_hex"].as_str().unwrap())),
                "response wrapper bytes mismatch: {e}"
            );
            assert_eq!(SubscriptionResponseWrapper::deserialize(&w.serialize()), w);
        }
    }

    fn behavior() -> serde_json::Value {
        serde_json::from_str(
            &std::fs::read_to_string("tests/testdata/fk_join/behavior.json").unwrap(),
        )
        .unwrap()
    }
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}

//! Thin [`Serde`] wrappers over the FK-join subscription wrappers, so they can
//! flow as typed `Record` values across the registration / response repartition
//! topics. The byte format is the JVM-exact `serialize`/`deserialize` of
//! [`SubscriptionWrapper`] / [`SubscriptionResponseWrapper`] (pinned by the
//! `--fkjoin` capture).

use bytes::Bytes;

use crate::dsl::processors::fk::subscription::{SubscriptionResponseWrapper, SubscriptionWrapper};
use crate::processor::serde::{Serde, SerdeAssociate, SerdeError};

/// `Serde<SubscriptionWrapper>` for the registration repartition topic value.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SubscriptionWrapperSerde;

impl Serde<SubscriptionWrapper> for SubscriptionWrapperSerde {
    fn serialize(&self, value: &SubscriptionWrapper) -> Bytes {
        value.serialize()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<SubscriptionWrapper, SerdeError> {
        Ok(SubscriptionWrapper::deserialize(bytes))
    }
}

impl SerdeAssociate for SubscriptionWrapperSerde {
    type Target = SubscriptionWrapper;
}

/// `Serde<SubscriptionResponseWrapper>` for the response repartition topic value.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SubscriptionResponseWrapperSerde;

impl Serde<SubscriptionResponseWrapper> for SubscriptionResponseWrapperSerde {
    fn serialize(&self, value: &SubscriptionResponseWrapper) -> Bytes {
        value.serialize()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<SubscriptionResponseWrapper, SerdeError> {
        Ok(SubscriptionResponseWrapper::deserialize(bytes))
    }
}

impl SerdeAssociate for SubscriptionResponseWrapperSerde {
    type Target = SubscriptionResponseWrapper;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::processors::fk::subscription::Instruction;
    use bytes::Bytes;

    #[test]
    fn subscription_wrapper_serde_round_trips() {
        let w = SubscriptionWrapper {
            instruction: Instruction::PropagateOnlyIfFkValAvailable,
            hash: Some(vec![1u8; 16]),
            primary_key: Bytes::from_static(b"pk"),
            primary_partition: 3,
        };
        let s = SubscriptionWrapperSerde;
        let b = s.serialize(&w);
        assert_eq!(s.deserialize(&b).unwrap(), w);
    }

    #[test]
    fn response_wrapper_serde_round_trips() {
        let w = SubscriptionResponseWrapper {
            hash: Some(vec![2u8; 16]),
            foreign_value: Some(Bytes::from_static(b"vfk")),
        };
        let s = SubscriptionResponseWrapperSerde;
        let b = s.serialize(&w);
        assert_eq!(s.deserialize(&b).unwrap(), w);
    }
}

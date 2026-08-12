//! Transaction administration.

use crabka_protocol::owned::{
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::FindCoordinatorResponse,
    init_producer_id_request::InitProducerIdRequest,
};
use crabka_units::convert::TimeExt as _;

use crate::{AdminClient, AdminError, kafka_error_name};

fn coordinator_address(
    transactional_id: &str,
    response: FindCoordinatorResponse,
) -> Result<String, AdminError> {
    if let Some(coordinator) = response
        .coordinators
        .into_iter()
        .find(|coordinator| coordinator.key == transactional_id)
    {
        if coordinator.error_code != 0 {
            return Err(AdminError::Broker {
                api: "FindCoordinator",
                code: coordinator.error_code,
                name: kafka_error_name(coordinator.error_code),
                message: coordinator.error_message,
            });
        }
        return Ok(format!("{}:{}", coordinator.host, coordinator.port));
    }

    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "FindCoordinator",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        });
    }
    if response.host.is_empty() {
        return Err(AdminError::Protocol(format!(
            "FindCoordinator returned no entry for transactional id {transactional_id:?}"
        )));
    }
    Ok(format!("{}:{}", response.host, response.port))
}

fn force_terminate_request(
    transactional_id: &str,
    transaction_timeout_ms: i32,
) -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: Some(transactional_id.to_owned()),
        transaction_timeout_ms,
        producer_id: -1,
        producer_epoch: -1,
        enable2_pc: false,
        keep_prepared_txn: false,
        ..Default::default()
    }
}

impl AdminClient {
    /// Fences the current producer generation and aborts any ongoing
    /// transaction for `transactional_id`.
    ///
    /// This is Kafka's `forceTerminateTransaction` operation: it discovers the
    /// transaction coordinator and sends `InitProducerId` with no producer
    /// identity and `keepPreparedTxn=false`. It is safe to call when no
    /// transaction is open; the coordinator still advances the producer
    /// generation so stale writers are fenced.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, or the
    /// coordinator lookup, connection, transport, and broker errors returned
    /// by Kafka.
    pub async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<(), AdminError> {
        if transactional_id.is_empty() {
            return Err(AdminError::Protocol(
                "transactional id must not be empty".to_owned(),
            ));
        }

        let response = self
            .conn
            .send(FindCoordinatorRequest {
                key: transactional_id.to_owned(),
                key_type: 1,
                coordinator_keys: vec![transactional_id.to_owned()],
                ..Default::default()
            })
            .await?;
        let coordinator = coordinator_address(transactional_id, response)?;
        let connection = Self::connect_one(&coordinator, self.options.clone()).await?;
        let response = connection
            .send(force_terminate_request(
                transactional_id,
                self.options.request_timeout.millis_i32(),
            ))
            .await?;
        if response.error_code != 0 {
            return Err(AdminError::Broker {
                api: "InitProducerId",
                code: response.error_code,
                name: kafka_error_name(response.error_code),
                message: None,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crabka_protocol::owned::find_coordinator_response::Coordinator;

    use super::*;

    #[test]
    fn force_termination_request_fences_without_preserving_transaction() {
        let request = force_terminate_request("payments", 30_000);

        assert2::assert!(request.transactional_id.as_deref() == Some("payments"));
        assert2::assert!(request.transaction_timeout_ms == 30_000);
        assert2::assert!(request.producer_id == -1);
        assert2::assert!(request.producer_epoch == -1);
        assert2::assert!(!request.enable2_pc);
        assert2::assert!(!request.keep_prepared_txn);
    }

    #[test]
    fn coordinator_lookup_selects_the_matching_batched_entry() {
        let response = FindCoordinatorResponse {
            coordinators: vec![
                Coordinator {
                    key: "other".to_owned(),
                    host: "wrong".to_owned(),
                    port: 1,
                    ..Default::default()
                },
                Coordinator {
                    key: "payments".to_owned(),
                    host: "coordinator".to_owned(),
                    port: 9092,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let address = coordinator_address("payments", response).expect("matching coordinator");
        assert2::assert!(address == "coordinator:9092");
    }
}

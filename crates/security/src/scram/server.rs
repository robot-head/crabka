//! `ScramServerExchange` — implemented in task 3.

use super::ScramCredential;
use crate::{AuthError, Principal};

#[derive(Debug)]
pub struct ScramServerExchange {
    _credential: ScramCredential,
}

#[derive(Debug)]
pub enum StepResult {
    Continue(Vec<u8>),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(credential: ScramCredential) -> Self {
        Self {
            _credential: credential,
        }
    }

    pub fn step(&mut self, _client_bytes: &[u8]) -> StepResult {
        StepResult::Failed(AuthError::MalformedMessage)
    }
}

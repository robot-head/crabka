//! Plan execution model.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{model::BalanceOperation, planner::Plan};

/// Planner execution policy after an operation does not apply cleanly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPolicy {
    /// Stop after the first failed or unsupported operation.
    #[default]
    StopOnFailure,
    /// Try every operation and report all outcomes.
    BestEffort,
}

/// Per-operation execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// The plan holds this operation, but the executor did not try it.
    Planned,
    /// The executor accepted the operation.
    Applied,
    /// The executor tried the operation and the operation failed.
    Failed,
    /// The executor has no typed hook for this operation kind.
    Unsupported,
}

/// One planned operation plus executor status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationExecution {
    pub operation: BalanceOperation,
    pub status: OperationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Result returned by a balancer executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReport {
    pub dry_run: bool,
    pub operations: Vec<BalanceOperation>,
    pub operation_results: Vec<OperationExecution>,
}

impl ExecutionReport {
    /// Return true when the executor applied every planned operation.
    #[must_use]
    pub fn is_fully_applied(&self) -> bool {
        self.operation_results
            .iter()
            .all(|result| result.status == OperationStatus::Applied)
    }

    /// Return true when any operation failed or lacks a live executor hook.
    #[must_use]
    pub fn has_terminal_error(&self) -> bool {
        self.operation_results.iter().any(|result| {
            matches!(
                result.status,
                OperationStatus::Failed | OperationStatus::Unsupported
            )
        })
    }
}

/// Error returned by a concrete operation executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    /// No typed hook exists for the planned operation.
    Unsupported { operation_name: &'static str },
    /// The typed hook exists, but the backend cannot execute it safely.
    UnsupportedMutation { message: String },
    /// The typed hook exists but failed.
    Failed { message: String },
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { operation_name } => {
                write!(
                    f,
                    "live executor does not support {operation_name} operations"
                )
            }
            Self::UnsupportedMutation { message } | Self::Failed { message } => {
                f.write_str(message)
            }
        }
    }
}

impl Error for ExecutionError {}

/// Typed operation executor used by live and test adapters.
pub trait BalanceExecutor {
    /// Return whether this executor only validates or reports without mutation.
    fn is_dry_run(&self) -> bool;

    /// Apply exactly one planned operation.
    ///
    /// # Errors
    ///
    /// This method returns [`ExecutionError::Unsupported`] when the executor has
    /// no typed hook for this operation kind.
    ///
    /// It returns [`ExecutionError::Failed`] when the hook rejects the
    /// operation.
    fn apply_operation(&mut self, operation: &BalanceOperation) -> Result<(), ExecutionError>;
}

/// Execute a plan through a typed executor seam.
#[must_use]
pub fn execute_plan<E: BalanceExecutor>(
    executor: &mut E,
    plan: &Plan,
    policy: ExecutionPolicy,
) -> ExecutionReport {
    let mut stopped = false;
    let mut operation_results = Vec::with_capacity(plan.operations.len());

    for operation in &plan.operations {
        if stopped {
            operation_results.push(planned_operation(operation));
            continue;
        }

        match executor.apply_operation(operation) {
            Ok(()) => operation_results.push(applied_operation(operation)),
            Err(error) => {
                operation_results.push(error_operation(operation, &error));
                stopped = policy == ExecutionPolicy::StopOnFailure;
            }
        }
    }

    ExecutionReport {
        dry_run: executor.is_dry_run(),
        operations: plan.operations.clone(),
        operation_results,
    }
}

/// Executor facade for the foundation batch.
#[derive(Debug, Clone, Copy)]
pub struct DryRunExecutor {
    dry_run: bool,
}

impl Default for DryRunExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DryRunExecutor {
    /// Build a dry-run executor.
    #[must_use]
    pub const fn new() -> Self {
        Self { dry_run: true }
    }

    /// Return exactly the planned operations without live registry mutation.
    #[must_use]
    pub fn execute(self, plan: &Plan) -> ExecutionReport {
        let operation_results = plan.operations.iter().map(planned_operation).collect();
        ExecutionReport {
            dry_run: self.dry_run,
            operations: plan.operations.clone(),
            operation_results,
        }
    }
}

/// Validate-only executor used until registry mutation hooks exist.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnsupportedExecutor;

impl BalanceExecutor for UnsupportedExecutor {
    fn is_dry_run(&self) -> bool {
        true
    }

    fn apply_operation(&mut self, operation: &BalanceOperation) -> Result<(), ExecutionError> {
        Err(ExecutionError::Unsupported {
            operation_name: operation.operation_name(),
        })
    }
}

/// Compatibility executor for callers that previously selected registry layout
/// execution.
///
/// This crate does not implement physical range orchestration, so this executor
/// rejects every operation before it tries any registry mutation.
pub struct RegistryLayoutExecutor<S> {
    store: S,
}

impl<S> RegistryLayoutExecutor<S> {
    /// Build a registry-layout executor over a registry store.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Return the wrapped store after execution.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.store
    }
}

impl<S> BalanceExecutor for RegistryLayoutExecutor<S> {
    fn is_dry_run(&self) -> bool {
        true
    }

    fn apply_operation(&mut self, operation: &BalanceOperation) -> Result<(), ExecutionError> {
        Err(ExecutionError::UnsupportedMutation {
            message: format!(
                "{} requires checkpoint, copy, catch-up, and cutover orchestration; physical range operations are unsupported",
                operation.operation_name()
            ),
        })
    }
}

fn planned_operation(operation: &BalanceOperation) -> OperationExecution {
    OperationExecution {
        operation: operation.clone(),
        status: OperationStatus::Planned,
        message: None,
    }
}

fn applied_operation(operation: &BalanceOperation) -> OperationExecution {
    OperationExecution {
        operation: operation.clone(),
        status: OperationStatus::Applied,
        message: None,
    }
}

fn error_operation(operation: &BalanceOperation, error: &ExecutionError) -> OperationExecution {
    OperationExecution {
        operation: operation.clone(),
        status: match error {
            ExecutionError::Unsupported { .. } | ExecutionError::UnsupportedMutation { .. } => {
                OperationStatus::Unsupported
            }
            ExecutionError::Failed { .. } => OperationStatus::Failed,
        },
        message: Some(error.to_string()),
    }
}

/// Map a registry control error into an executor error with fail-clear unsupported status.
#[must_use]
pub fn registry_execution_error(error: &crabka_gres_control::ControlError) -> ExecutionError {
    match error {
        crabka_gres_control::ControlError::UnsupportedRegistryMutation { .. } => {
            ExecutionError::UnsupportedMutation {
                message: error.to_string(),
            }
        }
        _ => ExecutionError::Failed {
            message: error.to_string(),
        },
    }
}

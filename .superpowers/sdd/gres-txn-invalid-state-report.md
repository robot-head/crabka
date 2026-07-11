# Gres transaction INVALID_TXN_STATE report

## Outcome

Fixed the live Gres startup failure where the first fence check returned Kafka
`INVALID_TXN_STATE` (24) and every later transaction attempt failed recovery
required.

## Root cause and protocol trace

`ProducerWalWriter::assert_current` opened and committed an empty client-side
transaction:

1. `InitProducerId` created the broker transaction entry in `Empty`.
2. `begin_transaction_owned` changed only the client state to `InTransaction`.
3. No `AddPartitionsToTxn` or transactional `Produce` occurred.
4. `EndTxn(commit)` attempted the broker transition `Empty -> PrepareCommit`.
5. `decide_phase1_transition` rejected that transition with error code 24.
6. Dropping the returned unresolved transaction guard marked the producer
   recovery-required, which explains the repeated unknown-outcome begin errors.

The fence check now commits an explicit empty GRW1 barrier through the existing
group-commit path. Transactional `Produce` performs the broker's inline
AddPartitions registration (`Empty -> Ongoing`) before `EndTxn`, and the same
path preserves send-error abort and fencing classification.

## TDD evidence

RED:

```text
cargo test -p crabka-gres-substrate --test checkpoint_service_runtime \
  live_fence_check_registers_a_transaction_before_end_txn -- --nocapture

fence check must produce before EndTxn: Unavailable("broker error_code 24")
test result: FAILED. 0 passed; 1 failed
```

GREEN:

```text
test live_fence_check_registers_a_transaction_before_end_txn ... ok
test result: ok. 1 passed; 0 failed
```

The regression also commits a normal group after the fence check, proving the
producer was not left wedged.

## Verification

- `checkpoint_service_runtime`: 3 passed.
- `crabka-gres-substrate --lib`: 102 passed.
- `cargo check -p crabka-gres-substrate`: passed.
- `cargo clippy -p crabka-gres-substrate --all-targets -- -D warnings` reached
  the crate but is blocked by the pre-existing, unrelated
  `clippy::manual_assert_eq` at `writer.rs:175`.

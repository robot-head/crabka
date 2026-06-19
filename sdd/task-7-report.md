# Task 7 Report: Rename Deny Test

## Fix: renamed misleading deny test

**Commit:** `78a3a131` - test(broker): rename deny test to match what it asserts

**Test name:** `denied_operation_returns_cluster_authorization_failed` (formerly `denied_operation_is_audited`)

**Test command output:**

```
running 3 tests
test audit_topic_exists_after_startup ... ok
test denied_operation_returns_cluster_authorization_failed ... ok
test broker_started_event_is_written_to_audit_topic ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

All tests passed.

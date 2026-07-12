# Gres G8 PausedAfterStage process evidence

The live Move process test was observed on 2026-07-12 with an actual Gres child SIGKILL
after the staged-tail digest became durable. The replacement PID differed, the Move completed,
146 acknowledged workload rows exactly matched the final database ledger, and predecessor WAL
retirement completed.

The observed maximum gap between consecutive fsynced acknowledgements was 4,530ms. CI uses a
7,000ms ceiling, providing 2,470ms (54%) headroom while keeping the 45-second operation deadline
as a separate invariant.

Command:

```text
CRABKA_G8_PROCESS_NEMESIS=1 CRABKA_G8_KILL_EVIDENCE=$PWD/target/g8-topology-process-nemesis/move-paused-stage-kill.json timeout 120s cargo test -q -p crabka-gres --test topology_process_nemesis -- --exact real_process_move_recovers_after_paused_stage_sigkill_with_exact_ack_ledger --nocapture
```

Observed result: `1 passed; 0 failed`, 35.36s.

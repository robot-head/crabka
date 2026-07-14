# Gres G8 source-phase process evidence

The live Move process test was observed on 2026-07-12 with an actual Gres child SIGKILL
after the staged-tail digest became durable. The replacement PID differed, the Move completed,
146 acknowledged workload rows exactly matched the final database ledger, and predecessor WAL
retirement completed.

The whole-operation deadline is a separate 75-second invariant from the phase-specific
acknowledgement continuity ceilings below.

Running must redo checkpoint, pause, and stage after restart. Three consecutive runs completed in
32.70s, 32.73s, and 34.11s with gaps of 7,122ms, 7,432ms, and 8,222ms. Cold restart was stable at
1,725-1,728ms; restart-ready to stage-complete took 6,430-6,885ms, identifying protocol replay as
the source of the wider gap. Identity-hardened and hosted-runner validation later observed gaps up
to 12,194ms, so CI uses a 15,000ms early-recovery ceiling with scheduling margin.

Checkpointed restarts after the manifest is durable but must still repeat pause, stage, markers,
and prologue. Three consecutive runs completed in 33.83s, 34.22s, and 32.85s with gaps of 8,117ms,
7,628ms, and 7,112ms. Cold restart was stable at 1,905-1,907ms; restart-ready to stage-complete
took 4,315-4,806ms. Hosted-runner validation later observed gaps up to 11,701ms, so this early
recovery case shares the 15,000ms ceiling.

PausedBeforeStage must repeat StageFilteredRestore after restart before it can publish the
successor serving snapshot. Three consecutive topology-before-table lock-order runs completed in
34.82s, 33.69s, and 34.54s with maximum acknowledgement gaps of 10,823ms, 10,218ms, and 10,218ms.
Their restart-ready to stage-complete intervals were 2,705ms, 2,180ms, and 2,192ms, and publication
followed 4,681ms, 4,641ms, and 4,706ms later. CI uses a 20,000ms ceiling
for PausedBeforeStage. Each run also committed a successor-bound acknowledgement after local
publication while registry cutover and predecessor retirement were still pending.

PausedAfterStage begins its outage before SIGKILL because Pause already holds the topology fence
while the test waits for durable Stage evidence. Three consecutive runs completed in 35.58s,
35.75s, and 35.60s with gaps of 11,806ms, 11,303ms, and 11,895ms. Kill to restart-ready took
4,179-4,205ms and restart-ready to publication took 4,670-5,667ms. Identity-hardened validation observed a 15,375ms gap, so CI uses a 20,000ms
PausedAfterStage ceiling, 3,105ms above the observed maximum.

Command:

```text
CRABKA_G8_PROCESS_NEMESIS=1 CRABKA_G8_SOURCE_KILL_POINT=paused_after_stage CRABKA_G8_KILL_EVIDENCE=$PWD/target/g8-topology-process-nemesis/move-paused_after_stage-kill.json timeout 120s cargo test -q -p crabka-gres --test topology_process_nemesis -- --exact real_process_move_source_phase_sigkill_with_exact_ack_ledger --nocapture
```

Observed result: `1 passed; 0 failed`, 35.36s.

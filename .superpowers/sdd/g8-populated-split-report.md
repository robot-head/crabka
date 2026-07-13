# G8 populated sharded split bridge report

Status: DONE_WITH_CONCERNS

## Architecture reuse

The deployed `crabka-gres::Runtime::split_successors` path already delegates to the production
`RangeTransferCapability`: it durably records activation intent and checkpoint identity, forces a
checkpoint, pauses at a durable barrier, reads the bounded committed tail, restores both filtered
successor intervals with physical-to-logical table mapping, rehomes timestamp descriptors and
intents to each target range, fences and claims both successors, and crosses the existing
fail-closed atomic activation protocol. Startup consumes the same durable activation receipt to
reconstruct and finish every post-activation phase. This change removes the tenant catalog gate
that prevented sharded and hash-sharded tables from reaching that machinery; foreign tables and
indexed tables remain fail-closed.

The legacy physically-empty helper remains data-free, but its evidence is now explicitly versioned
as `local-split-migration/v1/empty/r<id>/epoch<n>` instead of claiming an unversioned
`local-empty-table-no-data-migration` placeholder.

## RED / GREEN

RED: a populated `SHARDED` table through `MultiRangeTenant::split_successors` failed exactly with
`UnsupportedTableKind(TableId(10))` at the catalog validation boundary.

GREEN: the focused live runtime regression now creates
`SHARDED BY HASH (id) BUCKETS 16`, inserts two rows, executes the production physical split, and
asserts that both hash primary-version keys and the sequence key partition exactly across the two
successor folds. It passed.

## Commit

- `2e656607 feat(gres): bridge populated sharded splits to live transfer`
- this report is committed separately.

## Verification

- focused live populated hash transfer: 1 passed.
- legacy empty-table split: 1 passed.
- split model: 5 passed.
- split nemesis: 4 passed.
- topology process split crash binary: 23 passed, including its full kill-point/evidence matrix
  assertions. The run completed without launching an external live-process case in this environment,
  so it validates the binary and matrix but is not claimed as fresh multiprocess evidence.
- changed-file diff check: clean.

## Divergences and concerns

- The existing live regression is a whole-table boundary move (the degenerate split) and now covers
  pinned hash encoding. A new rowid-midpoint live split corpus with rows on both sides was not added.
- No fresh externally enabled multiprocess populated/hash crash run, conformance run, or all-target
  workspace check was completed in this slice.
- The lightweight in-process transfer fixture does not emulate the substrate checkpoint filter's
  timestamp-descriptor rehoming, so it was deliberately not used as evidence for sharded transfer.
  The live substrate regression exercises the real implementation.
- Marker/in-doubt preservation remains covered by the existing split model, nemesis, checkpoint
  filter tests, and 23-point crash binary; no new sharded marker fixture was added here.
- `crates/gres-ranges/src/control.rs` was preserved unstaged and unmodified by this slice.

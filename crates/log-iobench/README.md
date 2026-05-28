# crabka-log-iobench

Bench-only crate that answers one question: **would memory-mapping the log
segments (via `memmap2`/`memmapix`) speed up the fetch read path?**

It exists as a separate crate because the workspace sets
`unsafe_code = "forbid"`, and `Mmap::map` is `unsafe` by contract. This crate
opts out of the workspace lint set so the mmap path can be measured without
weakening the project-wide policy. (That forbid is itself a finding: today
`memmap2` is a declared dependency that *cannot be called* from any crate that
inherits the workspace lints.)

## Running

```sh
cargo bench --bench mmap_read -p crabka-log-iobench
# quick pass:
cargo bench --bench mmap_read -p crabka-log-iobench -- --warm-up-time 1 --measurement-time 3
```

## What it measures

`segment_io/*` isolates the raw byte-fetch from a sealed `.log` segment — the
only part mmap would actually replace — for a 1 MiB (default fetch) and a
16 KiB read:

| variant            | what it does                                              |
|--------------------|-----------------------------------------------------------|
| `pread_to_vec`     | current behaviour: `seek` + `read_to_end` into a fresh Vec |
| `pread_reuse_buf`  | same syscall, buffer reused across reads                  |
| `mmap_once_copy`   | map once, copy the range into a Vec each read             |
| `mmap_once_borrow` | map once, read the range in place (no copy)¹              |
| `mmap_per_call`    | map+unmap each read (lazy-mapping overhead)               |

`full_path/log_read_1MiB_decoded` runs the real `Log::read` (index lookup +
I/O + batch **decode**) so the raw I/O can be read in context.

## Results (warm page cache, dev box, 2026-05)

| strategy           | 1 MiB read | 16 KiB read |
|--------------------|-----------:|------------:|
| `pread_to_vec`     |    68.1 µs |     1.66 µs |
| `pread_reuse_buf`  |    68.0 µs |     1.54 µs |
| `mmap_once_copy`   |    47.2 µs |     0.18 µs |
| `mmap_once_borrow`¹|     211 µs |     3.19 µs |
| `mmap_per_call`    |     264 µs |    16.4 µs |
| **`full_path` (decoded)** | **719 µs** | — |

¹ `mmap_once_borrow`'s time is dominated by the byte-sum checksum used to stop
the optimizer eliding the read — it is **not** a "zero-copy is slow" signal. It
just confirms that *if you still touch every byte* (which decoding does), mmap
saves nothing. A true zero-copy win needs a `sendfile`/`splice` path that never
touches the bytes in user space.

## Conclusions

1. **The decode/re-encode dominates, not the I/O.** The full read path is
   ~719 µs; the raw I/O it contains is ~68 µs (~10%). Making I/O *free* would
   shave <10% off a fetch. mmap's actual saving (≈21 µs on the 1 MiB read)
   is ~3% of the end-to-end read. Not worth relaxing `unsafe_code = "forbid"`
   on its own.

2. **mmap only wins if you map once and cache it.** `mmap_once_copy` beats
   `pread` (47 vs 68 µs at 1 MiB; 0.18 vs 1.66 µs at 16 KiB), but
   `mmap_per_call` is a trap — map+unmap costs ~200 µs of page-table churn,
   making lazy per-fetch mapping ~4× *slower* than the current `pread`.

3. **The real lever is eliminating the decode and adding zero-copy-to-socket**
   (`sendfile`/`splice`), which is a much larger change than swapping in a
   mapping crate. mmap of segments is only worth doing bundled with that.

4. **Where mmap is independently worth it** (not measured here, lower risk):
   the **index files** (`index.rs`, `txn_index.rs`), which currently
   `read_to_end` the whole file into a Vec at open — mmap cuts open cost and
   memory at scale (this is exactly what Kafka mmaps), and **compaction**,
   which slurps whole segments into the heap.

5. **`memmapix` vs `memmap2`:** `memmapix` is `memmap2` reimplemented on
   `rustix` instead of `libc` — same `mmap(2)` underneath, no performance
   edge. If mmap is ever adopted, use the `memmap2` already in the tree; there
   is no throughput reason to switch to `memmapix`.

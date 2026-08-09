# crabka-log-iobench

Bench-only crate that answers one question: **does a memory map of the log
segments, through `memmap2`/`memmapix`, make the fetch read path faster?**

It exists as a separate crate because the workspace sets
`unsafe_code = "forbid"`, and `Mmap::map` is `unsafe` by contract. This crate
opts out of the workspace lint set, so the benchmark can measure the mmap path
and keep the project-wide policy. That forbid is itself a finding: `memmap2` is
a declared dependency that *cannot be called* from any crate that inherits the
workspace lints.

## Running

```sh
cargo bench --bench mmap_read -p crabka-log-iobench
# quick pass:
cargo bench --bench mmap_read -p crabka-log-iobench -- --warm-up-time 1 --measurement-time 3
```

## What it measures

`segment_io/*` isolates the raw byte-fetch from a sealed `.log` segment for a
1 MiB read and a 16 KiB read. The 1 MiB read is the default fetch. This
byte-fetch is the only part that mmap would replace:

| variant            | what it does                                              |
|--------------------|-----------------------------------------------------------|
| `pread_to_vec`     | current behaviour: `seek` + `read_to_end` into a fresh Vec |
| `pread_reuse_buf`  | same syscall, buffer reused across reads                  |
| `mmap_once_copy`   | map once, copy the range into a Vec each read             |
| `mmap_once_borrow` | map once, read the range in place (no copy)¹              |
| `mmap_per_call`    | map+unmap each read (lazy-mapping overhead)               |

`full_path/log_read_1MiB_decoded` runs the real `Log::read`, which does an index
lookup, the I/O, and the batch **decode**. You can then read the raw I/O numbers
in context.

## Results (warm page cache, dev box, 2026-05)

| strategy           | 1 MiB read | 16 KiB read |
|--------------------|-----------:|------------:|
| `pread_to_vec`     |    68.1 µs |     1.66 µs |
| `pread_reuse_buf`  |    68.0 µs |     1.54 µs |
| `mmap_once_copy`   |    47.2 µs |     0.18 µs |
| `mmap_once_borrow`¹|     211 µs |     3.19 µs |
| `mmap_per_call`    |     264 µs |    16.4 µs |
| **`full_path` (decoded)** | **719 µs** | — |

¹ The byte-sum checksum dominates `mmap_once_borrow`'s time. That checksum makes
sure the optimizer does not elide the read. The number is **not** a "zero-copy is
slow" signal. It only shows that mmap saves nothing *if you still touch every
byte*, and the decode does touch every byte. A true zero-copy win needs a
`sendfile`/`splice` path that never touches the bytes in user space.

## Conclusions

1. **The decode/re-encode dominates, not the I/O.** The full read path is
   ~719 µs. The raw I/O in it is ~68 µs, which is ~10%. Free I/O would remove
   <10% of a fetch. The mmap saving is ≈21 µs on the 1 MiB read, which is ~3%
   of the end-to-end read. That is not a good enough reason on its own to relax
   `unsafe_code = "forbid"`.

2. **mmap only wins if you map once and cache it.** `mmap_once_copy` is faster
   than `pread`: 47 against 68 µs at 1 MiB, and 0.18 against 1.66 µs at 16 KiB.
   But `mmap_per_call` is much worse. Its map and unmap cost ~200 µs of
   page-table churn. A lazy map for each fetch is thus ~4× *slower* than the
   current `pread`.

3. **The largest gain comes from removal of the decode and a
   zero-copy-to-socket path** (`sendfile`/`splice`). That is a much larger
   change than a new mapping crate. Only do the mmap of segments together with
   that change.

4. **Where mmap is independently worth it.** These two cases are not measured
   here and have lower risk. The **index files** (`index.rs`, `txn_index.rs`)
   `read_to_end` the whole file into a Vec at open, and mmap cuts the open cost
   and the memory at scale. Kafka mmaps these same files. **Compaction** also
   reads whole segments into the heap.

5. **`memmapix` vs `memmap2`:** `memmapix` is a reimplementation of `memmap2` on
   `rustix` instead of `libc`. It uses the same `mmap(2)` and gives no
   performance advantage. Use the `memmap2` already in the tree if the project
   ever adopts mmap. There is no throughput reason to change to `memmapix`.

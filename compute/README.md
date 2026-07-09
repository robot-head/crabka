# Compute scaffold

The `compute/` tree contains the in-repo PG5 compute artifacts that can exist
before the external live-boot gates are available:

- `compute/patches/pg17/0001-smgr-hook.patch` is the vendored PostgreSQL 17 smgr
  hook patch. It adds the `smgr_hook_type` extension hook and keeps upstream
  `md.c` as the default storage manager.
- `compute/extension/` contains the PGXS extension scaffold. `_PG_init` declares
  the `crabka.pageserver_endpoint`, `crabka.tenant`, and `crabka.timeline` GUCs,
  connects through the checked-in C ABI, and installs the patched smgr hook.
- `compute/image/build.sh` documents the packaging path: fetch pinned PG17
  sources, apply the patch, build Postgres, build `crabka-compute-client`, build
  the extension, and assemble an image.
- `compute/include/crabka_compute_client.h` mirrors the Rust `repr(C)` structs
  and exported FFI entry points in `crates/compute-client/src/ffi.rs`.
- `compute/scripts/check-artifacts.sh` verifies the artifact shape without
  requiring Docker or a live Postgres boot. Set `PG17_SOURCE_DIR=/path/to/pg17`
  to additionally run `git apply --check` for the smgr patch.

This is not a PG5 live completion claim. The following gates remain external:

1. A real patched PostgreSQL 17 source corpus accepted by
   `git apply --check` and a full `make check` run.
2. A Docker/OCI image build in an environment with the required PG17 build
   dependencies and the release `crabka-compute-client` cdylib.
3. Pageserver live boot and pgbench against the disaggregated stack, still gated
   by PG4b/real PG17 SLRU and basebackup readiness.

The Rust C ABI currently validates request layout and maps invalid caller input
to stable negative result codes; real IO still returns an explicit unsupported
code so callers cannot accidentally treat the scaffold as live transport.

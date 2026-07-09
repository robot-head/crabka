//! Generates pageserver protobuf messages and blocks checked-in C header drift.

use std::{env, error::Error, fs, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    compile_pageserver_proto()?;
    check_header_drift()?;
    Ok(())
}

fn compile_pageserver_proto() -> Result<(), Box<dyn Error>> {
    let proto = "../pageserver/proto/crabka/pageserver/v1/pageserver.proto";
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    prost_build::Config::new()
        .protoc_executable(protoc_path)
        .compile_protos(&[proto], &["../pageserver/proto"])?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

fn check_header_drift() -> Result<(), Box<dyn Error>> {
    // cbindgen is intentionally not required for this narrow ABI: the exported
    // header includes human-curated result-code constants and an opaque handle,
    // while Rust-side tests compile the checked-in C/C++ header and verify
    // repr(C) sizes/offsets. Keep this generated literal in lockstep so header
    // edits still block the build unless the ABI update is explicit.
    let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    let header_path =
        Path::new(&manifest_dir).join("../../compute/include/crabka_compute_client.h");
    let checked_in_header = fs::read_to_string(&header_path)?;
    if checked_in_header != expected_header() {
        return Err(format!(
            "{} drifted from crates/compute-client/build.rs expected_header(); update the FFI ABI intentionally",
            header_path.display()
        )
        .into());
    }
    println!("cargo:rerun-if-changed={}", header_path.display());
    Ok(())
}

fn expected_header() -> &'static str {
    r#"/*
 * Crabka compute/pageserver FFI boundary.
 *
 * This header is the checked-in C ABI contract for crabka-compute-client. The
 * Rust crate owns the matching repr(C) layouts and exposes a deterministic drift
 * check so this file changes only with an intentional ABI update.
 */

#ifndef CRABKA_COMPUTE_CLIENT_H
#define CRABKA_COMPUTE_CLIENT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CRABKA_COMPUTE_FFI_VERSION 1U

#define CRABKA_COMPUTE_OPERATION_UNKNOWN 0U
#define CRABKA_COMPUTE_OPERATION_SEED_TIMELINE 1U
#define CRABKA_COMPUTE_OPERATION_START_BASEBACKUP 2U
#define CRABKA_COMPUTE_OPERATION_FETCH_PAGE 3U

#define CRABKA_COMPUTE_FORK_MAIN 0U
#define CRABKA_COMPUTE_FORK_FREE_SPACE_MAP 1U
#define CRABKA_COMPUTE_FORK_VISIBILITY_MAP 2U
#define CRABKA_COMPUTE_FORK_INIT 3U

#define CRABKA_COMPUTE_STATUS_OK 0U
#define CRABKA_COMPUTE_STATUS_INVALID_ARGUMENT 1U

#define CRABKA_COMPUTE_RESULT_OK 0
#define CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT -1
#define CRABKA_COMPUTE_RESULT_INTERNAL_ERROR -2

#define CRABKA_COMPUTE_PAGE_SIZE 8192U

typedef struct CrabkaComputeBorrowedBytes {
    const char *ptr;
    size_t len;
} CrabkaComputeBorrowedBytes;

typedef struct CrabkaComputeTimelineSeedRequest {
    uint32_t version;
    CrabkaComputeBorrowedBytes tenant_id;
    CrabkaComputeBorrowedBytes timeline_id;
    CrabkaComputeBorrowedBytes ancestor_timeline_id;
    uint64_t ancestor_start_lsn;
} CrabkaComputeTimelineSeedRequest;

typedef struct CrabkaComputeBasebackupRequest {
    uint32_t version;
    CrabkaComputeBorrowedBytes tenant_id;
    CrabkaComputeBorrowedBytes timeline_id;
    uint64_t lsn;
} CrabkaComputeBasebackupRequest;

typedef struct CrabkaComputePageFetchRequest {
    uint32_t version;
    CrabkaComputeBorrowedBytes tenant_id;
    CrabkaComputeBorrowedBytes timeline_id;
    uint32_t tablespace_oid;
    uint32_t database_oid;
    uint32_t relfilenode;
    uint32_t fork_name;
    uint32_t block_number;
    uint64_t request_lsn;
} CrabkaComputePageFetchRequest;

typedef struct CrabkaComputeResponse {
    uint32_t version;
    uint32_t status;
    void *payload_ptr;
    size_t payload_len;
    CrabkaComputeBorrowedBytes error_message;
} CrabkaComputeResponse;

typedef struct CrabkaComputeClient CrabkaComputeClient;

int32_t ck_connect(const char *endpoint, CrabkaComputeClient **handle_out);
int32_t ck_get_page(
    CrabkaComputeClient *handle,
    const CrabkaComputePageFetchRequest *request,
    uint8_t *out_page,
    size_t out_page_len);
void ck_disconnect(CrabkaComputeClient *handle);
CrabkaComputeBorrowedBytes ck_last_error_message(void);

#ifdef __cplusplus
}
#endif

#endif /* CRABKA_COMPUTE_CLIENT_H */
"#
}

/*
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

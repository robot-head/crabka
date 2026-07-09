/*
 * Crabka PostgreSQL compute storage manager hook.
 *
 * This extension wires shared_preload_libraries, GUCs, the PG17 smgr method
 * table, and blocking page fetches through crabka-compute-client. The external
 * patched-PostgreSQL boot and pgbench gate remains manual until a PG17 source
 * corpus and pageserver fixture are available in CI.
 */

#include "postgres.h"

#include "access/xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"

#include "crabka_compute_client.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static char *crabka_pageserver_endpoint = NULL;
static char *crabka_tenant_id = NULL;
static char *crabka_timeline_id = NULL;
static CrabkaComputeClient *crabka_client = NULL;
static smgr_hook_type previous_smgr_hook = NULL;
static const f_smgr *crabka_default_smgr = NULL;

static const f_smgr *crabka_default_smgr_or_error(void);
static void crabka_smgr_open(SMgrRelation reln);
static void crabka_smgr_close(SMgrRelation reln, ForkNumber forknum);
static void crabka_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
static bool crabka_smgr_exists(SMgrRelation reln, ForkNumber forknum);
static void crabka_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
static void crabka_smgr_extend(SMgrRelation reln, ForkNumber forknum,
                               BlockNumber blocknum, const void *buffer, bool skipFsync);
static void crabka_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum,
                                   BlockNumber blocknum, int nblocks, bool skipFsync);
static bool crabka_smgr_prefetch(SMgrRelation reln, ForkNumber forknum,
                                 BlockNumber blocknum, int nblocks);
static void crabka_smgr_readv(SMgrRelation reln, ForkNumber forknum,
                              BlockNumber blocknum, void **buffers, BlockNumber nblocks);
static void crabka_smgr_writev(SMgrRelation reln, ForkNumber forknum,
                               BlockNumber blocknum, const void **buffers,
                               BlockNumber nblocks, bool skipFsync);
static void crabka_smgr_writeback(SMgrRelation reln, ForkNumber forknum,
                                  BlockNumber blocknum, BlockNumber nblocks);
static BlockNumber crabka_smgr_nblocks(SMgrRelation reln, ForkNumber forknum);
static void crabka_smgr_truncate(SMgrRelation reln, ForkNumber forknum,
                                 BlockNumber old_blocks, BlockNumber nblocks);
static void crabka_smgr_immedsync(SMgrRelation reln, ForkNumber forknum);
static void crabka_smgr_registersync(SMgrRelation reln, ForkNumber forknum);

static const f_smgr crabka_smgr = {
    .smgr_init = NULL,
    .smgr_shutdown = NULL,
    .smgr_open = crabka_smgr_open,
    .smgr_close = crabka_smgr_close,
    .smgr_create = crabka_smgr_create,
    .smgr_exists = crabka_smgr_exists,
    .smgr_unlink = crabka_smgr_unlink,
    .smgr_extend = crabka_smgr_extend,
    .smgr_zeroextend = crabka_smgr_zeroextend,
    .smgr_prefetch = crabka_smgr_prefetch,
    .smgr_readv = crabka_smgr_readv,
    .smgr_writev = crabka_smgr_writev,
    .smgr_writeback = crabka_smgr_writeback,
    .smgr_nblocks = crabka_smgr_nblocks,
    .smgr_truncate = crabka_smgr_truncate,
    .smgr_immedsync = crabka_smgr_immedsync,
    .smgr_registersync = crabka_smgr_registersync,
};

static const f_smgr *crabka_select_smgr(SMgrRelation reln, const f_smgr *default_smgr);
static const char *crabka_required_guc(const char *name, const char *value);
static void crabka_connect_or_error(void);
static CrabkaComputeBorrowedBytes crabka_borrowed_text(const char *value);
static uint32 crabka_fork_number_or_error(ForkNumber forknum);
static void crabka_fetch_page_or_error(SMgrRelation reln, ForkNumber forknum,
                                       BlockNumber blocknum, void *buffer);

void
_PG_init(void)
{
    DefineCustomStringVariable(
        "crabka.pageserver_endpoint",
        "Crabka pageserver endpoint used by the compute smgr hook.",
        NULL,
        &crabka_pageserver_endpoint,
        NULL,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);
    DefineCustomStringVariable(
        "crabka.tenant",
        "Crabka tenant identifier used for page-service requests.",
        NULL,
        &crabka_tenant_id,
        NULL,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);
    DefineCustomStringVariable(
        "crabka.timeline",
        "Crabka timeline identifier used for page-service requests.",
        NULL,
        &crabka_timeline_id,
        NULL,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);

    crabka_connect_or_error();
    previous_smgr_hook = smgr_hook;
    smgr_hook = crabka_select_smgr;
}

static const f_smgr *
crabka_select_smgr(SMgrRelation reln, const f_smgr *default_smgr)
{
    crabka_default_smgr = default_smgr;

    if (previous_smgr_hook != NULL)
    {
        const f_smgr *selected_smgr = previous_smgr_hook(reln, default_smgr);
        if (selected_smgr != NULL && selected_smgr != default_smgr)
            return selected_smgr;
    }

    if (RelFileLocatorSkippingWAL(reln->smgr_rlocator.locator))
        return default_smgr;

    return &crabka_smgr;
}

static const f_smgr *
crabka_default_smgr_or_error(void)
{
    if (crabka_default_smgr == NULL)
        ereport(ERROR,
                (errmsg("Crabka smgr hook was called before PostgreSQL supplied the default smgr")));

    return crabka_default_smgr;
}

static void
crabka_smgr_open(SMgrRelation reln)
{
    crabka_default_smgr_or_error()->smgr_open(reln);
}

static void
crabka_smgr_close(SMgrRelation reln, ForkNumber forknum)
{
    crabka_default_smgr_or_error()->smgr_close(reln, forknum);
}

static void
crabka_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    crabka_default_smgr_or_error()->smgr_create(reln, forknum, isRedo);
}

static bool
crabka_smgr_exists(SMgrRelation reln, ForkNumber forknum)
{
    return crabka_default_smgr_or_error()->smgr_exists(reln, forknum);
}

static void
crabka_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
    crabka_default_smgr_or_error()->smgr_unlink(rlocator, forknum, isRedo);
}

static void
crabka_smgr_extend(SMgrRelation reln, ForkNumber forknum,
                   BlockNumber blocknum, const void *buffer, bool skipFsync)
{
    crabka_default_smgr_or_error()->smgr_extend(reln, forknum, blocknum, buffer, skipFsync);
}

static void
crabka_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber blocknum, int nblocks, bool skipFsync)
{
    crabka_default_smgr_or_error()->smgr_zeroextend(reln, forknum, blocknum, nblocks, skipFsync);
}

static bool
crabka_smgr_prefetch(SMgrRelation reln, ForkNumber forknum,
                     BlockNumber blocknum, int nblocks)
{
    return crabka_default_smgr_or_error()->smgr_prefetch(reln, forknum, blocknum, nblocks);
}

static void
crabka_smgr_readv(SMgrRelation reln, ForkNumber forknum,
                  BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
    BlockNumber offset;

    if (crabka_client == NULL)
    {
        crabka_default_smgr_or_error()->smgr_readv(reln, forknum, blocknum, buffers, nblocks);
        return;
    }

    for (offset = 0; offset < nblocks; offset++)
        crabka_fetch_page_or_error(reln, forknum, blocknum + offset, buffers[offset]);
}

static void
crabka_smgr_writev(SMgrRelation reln, ForkNumber forknum,
                   BlockNumber blocknum, const void **buffers,
                   BlockNumber nblocks, bool skipFsync)
{
    crabka_default_smgr_or_error()->smgr_writev(reln, forknum, blocknum, buffers, nblocks, skipFsync);
}

static void
crabka_smgr_writeback(SMgrRelation reln, ForkNumber forknum,
                      BlockNumber blocknum, BlockNumber nblocks)
{
    crabka_default_smgr_or_error()->smgr_writeback(reln, forknum, blocknum, nblocks);
}

static BlockNumber
crabka_smgr_nblocks(SMgrRelation reln, ForkNumber forknum)
{
    return crabka_default_smgr_or_error()->smgr_nblocks(reln, forknum);
}

static void
crabka_smgr_truncate(SMgrRelation reln, ForkNumber forknum,
                     BlockNumber old_blocks, BlockNumber nblocks)
{
    crabka_default_smgr_or_error()->smgr_truncate(reln, forknum, old_blocks, nblocks);
}

static void
crabka_smgr_immedsync(SMgrRelation reln, ForkNumber forknum)
{
    crabka_default_smgr_or_error()->smgr_immedsync(reln, forknum);
}

static void
crabka_smgr_registersync(SMgrRelation reln, ForkNumber forknum)
{
    crabka_default_smgr_or_error()->smgr_registersync(reln, forknum);
}

static const char *
crabka_required_guc(const char *name, const char *value)
{
    if (value == NULL || value[0] == '\0')
        ereport(ERROR,
                (errmsg("%s must be set before loading crabka_compute", name)));

    return value;
}

static void
crabka_connect_or_error(void)
{
    const char *endpoint = crabka_required_guc(
        "crabka.pageserver_endpoint",
        crabka_pageserver_endpoint);

    crabka_required_guc("crabka.tenant", crabka_tenant_id);
    crabka_required_guc("crabka.timeline", crabka_timeline_id);

    if (ck_connect(endpoint, &crabka_client) != CRABKA_COMPUTE_RESULT_OK)
    {
        CrabkaComputeBorrowedBytes message = ck_last_error_message();

        ereport(ERROR,
                (errmsg("could not connect Crabka compute client"),
                 errdetail_internal("%.*s", (int) message.len, message.ptr)));
    }
}

static CrabkaComputeBorrowedBytes
crabka_borrowed_text(const char *value)
{
    CrabkaComputeBorrowedBytes bytes;

    bytes.ptr = value;
    bytes.len = strlen(value);
    return bytes;
}

static uint32
crabka_fork_number_or_error(ForkNumber forknum)
{
    if (forknum < 0 || forknum > CRABKA_COMPUTE_FORK_INIT)
        ereport(ERROR,
                (errmsg("Crabka cannot fetch unsupported fork number %d", forknum)));

    return (uint32) forknum;
}

static void
crabka_fetch_page_or_error(SMgrRelation reln, ForkNumber forknum,
                           BlockNumber blocknum, void *buffer)
{
    CrabkaComputePageFetchRequest request;
    int32 result;

    request.version = CRABKA_COMPUTE_FFI_VERSION;
    request.tenant_id = crabka_borrowed_text(crabka_required_guc("crabka.tenant", crabka_tenant_id));
    request.timeline_id = crabka_borrowed_text(crabka_required_guc("crabka.timeline", crabka_timeline_id));
    request.tablespace_oid = reln->smgr_rlocator.locator.spcOid;
    request.database_oid = reln->smgr_rlocator.locator.dbOid;
    request.relfilenode = reln->smgr_rlocator.locator.relNumber;
    request.fork_name = crabka_fork_number_or_error(forknum);
    request.block_number = blocknum;
    request.request_lsn = GetXLogInsertRecPtr();

    result = ck_get_page(crabka_client, &request, buffer, BLCKSZ);
    if (result != CRABKA_COMPUTE_RESULT_OK)
    {
        CrabkaComputeBorrowedBytes message = ck_last_error_message();

        ereport(ERROR,
                (errmsg("could not fetch Crabka page from pageserver"),
                 errdetail_internal("%.*s", (int) message.len, message.ptr)));
    }
}

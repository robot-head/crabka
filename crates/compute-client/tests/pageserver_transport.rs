use std::{ffi::CString, ptr, sync::Arc};

use assert2::assert;
use bytes::Bytes;
use crabka_compute_client::{
    BasebackupRequest, BlockNumber, BlockingPageServerClient, ComputePageServerClient, DatabaseOid,
    ForkName, LiveWalIngestRequest, Lsn, PageFetchRequest, PageSeedImageRequest, RelFileNode,
    TablespaceOid, TenantId, TimelineId, TimelineSeedRequest, ffi,
};
use crabka_page_store::{ForkNumber, PAGE_SIZE, PageKey, RelTag};
use crabka_pageserver::{
    InMemoryTimelineStore, PageService, PostgresRedo, PutPageImageRequest, SyntheticRedoCodec,
    TimelineKey, connect_router,
};
use crabka_postgres_redo::RedoRecord;
use crabka_postgres_wal::Lsn as StoreLsn;

#[tokio::test]
async fn blocking_transport_fetches_page_through_connect_router() {
    let timeline = timeline_key("tenant-a", "timeline-a");
    let page_key = page_key();
    let page = page_image(42);
    let service = Arc::new(service_with_timeline(&timeline));
    service
        .put_page_image(PutPageImageRequest {
            timeline: timeline.clone(),
            key: page_key,
            lsn: StoreLsn(10),
            page: Bytes::from(page.clone()),
        })
        .await
        .expect("page image seed succeeds");
    service
        .store_mut()
        .await
        .put_relation_size(&timeline, page_key.rel, StoreLsn(10), 1)
        .expect("relation size seed succeeds");
    let endpoint = serve_pageserver(service).await;

    let fetched = tokio::task::spawn_blocking(move || {
        let client = BlockingPageServerClient::connect(&endpoint)?;
        client.fetch_page(PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-a")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(0),
            request_lsn: Lsn::new(10),
        })
    })
    .await
    .expect("blocking transport task completes")
    .expect("page fetch succeeds");

    assert!(fetched.bytes == page);
}

#[tokio::test]
async fn blocking_transport_seeds_branch_and_reads_basebackup_payload() {
    let source_timeline = timeline_key("tenant-a", "timeline-root");
    let page_key = page_key();
    let page = page_image(99);
    let service = Arc::new(service_with_timeline(&source_timeline));
    service
        .put_page_image(PutPageImageRequest {
            timeline: source_timeline.clone(),
            key: page_key,
            lsn: StoreLsn(10),
            page: Bytes::from(page.clone()),
        })
        .await
        .expect("source page image seed succeeds");
    service
        .store_mut()
        .await
        .put_relation_size(&source_timeline, page_key.rel, StoreLsn(10), 1)
        .expect("source relation size seed succeeds");
    let endpoint = serve_pageserver(service).await;

    let (branched_page, basebackup) = tokio::task::spawn_blocking(move || {
        let client = BlockingPageServerClient::connect(&endpoint)?;
        client.seed_timeline(TimelineSeedRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-child")?,
            ancestor_timeline_id: TimelineId::try_from("timeline-root")?,
            ancestor_start_lsn: Lsn::new(10),
        })?;
        let branched_page = client.fetch_page(PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-child")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(0),
            request_lsn: Lsn::new(10),
        })?;
        let basebackup = client.start_basebackup(BasebackupRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-child")?,
            lsn: Lsn::new(10),
        })?;
        Ok::<_, crabka_compute_client::ComputeClientError>((branched_page, basebackup))
    })
    .await
    .expect("blocking transport task completes")
    .expect("seed and basebackup requests succeed");

    assert!(branched_page.bytes == page);
    assert!(basebackup.lsn == Lsn::new(10));
    assert!(basebackup.tar.starts_with(b"CRABKA_BASEBACKUP_V1\n"));
    assert!(
        basebackup.stream_id.as_str()
            == format!(
                "basebackup:tenant-a:timeline-child:{}",
                basebackup.tar.len()
            )
    );
    assert!(basebackup.relmeta_count == 1);
    assert!(basebackup.slru_page_count == 0);
}

#[tokio::test]
async fn ffi_handle_fetches_page_through_blocking_transport() {
    let timeline = timeline_key("tenant-a", "timeline-a");
    let page_key = page_key();
    let page = page_image(7);
    let service = Arc::new(service_with_timeline(&timeline));
    service
        .put_page_image(PutPageImageRequest {
            timeline: timeline.clone(),
            key: page_key,
            lsn: StoreLsn(10),
            page: Bytes::from(page.clone()),
        })
        .await
        .expect("page image seed succeeds");
    service
        .store_mut()
        .await
        .put_relation_size(&timeline, page_key.rel, StoreLsn(10), 1)
        .expect("relation size seed succeeds");
    let endpoint = serve_pageserver(service).await;

    let fetched = tokio::task::spawn_blocking(move || {
        let endpoint = CString::new(endpoint).expect("endpoint does not contain NUL");
        let mut handle = ptr::null_mut();
        // SAFETY: `endpoint` is NUL-terminated and `handle` is a valid out pointer.
        let connect_result = unsafe { ffi::ck_connect(endpoint.as_ptr(), &raw mut handle) };
        assert!(connect_result == ffi::CRABKA_COMPUTE_RESULT_OK);
        assert!(!handle.is_null());

        let tenant_id = "tenant-a";
        let timeline_id = "timeline-a";
        let request = ffi::CrabkaComputePageFetchRequest {
            version: ffi::CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes(tenant_id),
            timeline_id: borrowed_bytes(timeline_id),
            tablespace_oid: 1663,
            database_oid: 5,
            relfilenode: 42,
            fork_name: ffi::CRABKA_COMPUTE_FORK_MAIN,
            block_number: 0,
            request_lsn: 10,
        };
        let mut fetched = vec![0; ffi::CRABKA_COMPUTE_PAGE_SIZE];
        // SAFETY: `handle` came from `ck_connect`, request borrows live strings,
        // and `fetched` is exactly one page long.
        let result = unsafe {
            ffi::ck_get_page(
                handle,
                &raw const request,
                fetched.as_mut_ptr(),
                fetched.len(),
            )
        };
        // SAFETY: ownership of the non-null handle is returned exactly once.
        unsafe { ffi::ck_disconnect(handle) };
        assert!(result == ffi::CRABKA_COMPUTE_RESULT_OK);
        fetched
    })
    .await
    .expect("blocking FFI transport task completes");

    assert!(fetched == page);
}

#[tokio::test]
async fn blocking_transport_seeds_image_and_ingests_live_wal_through_connect_router() {
    let timeline = timeline_key("tenant-a", "timeline-live");
    let page_key = page_key();
    let initial_page = page_image(5);
    let patched_page = page_image(90);
    let service = Arc::new(service_with_timeline(&timeline));
    let endpoint = serve_pageserver(service).await;

    let fetched = tokio::task::spawn_blocking(move || {
        let client = BlockingPageServerClient::connect(&endpoint)?;
        client.seed_page_image(PageSeedImageRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-live")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(0),
            lsn: Lsn::new(10),
            page: initial_page,
        })?;

        let record = RedoRecord::full_page_image(page_key, StoreLsn(30), Bytes::from(patched_page));
        client.ingest_wal(LiveWalIngestRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-live")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(0),
            lsn: Lsn::new(30),
            will_init: false,
            record: SyntheticRedoCodec::encode_record(&record).to_vec(),
        })?;

        client.fetch_page(PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-live")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(0),
            request_lsn: Lsn::new(30),
        })
    })
    .await
    .expect("blocking transport task completes")
    .expect("live WAL fetch succeeds");

    assert!(fetched.bytes[0] == 90);
}

#[test]
#[ignore = "manual PG17 patched-compute boot plus pgbench gate; requires PG17_SOURCE_DIR, CRABKA_COMPUTE_IMAGE, and a live pageserver endpoint"]
fn manual_pg17_compute_boot_pgbench_gate_requires_external_fixture() {
    for variable in [
        "PG17_SOURCE_DIR",
        "CRABKA_COMPUTE_IMAGE",
        "CRABKA_PAGESERVER_ENDPOINT",
    ] {
        assert!(
            std::env::var_os(variable).is_some(),
            "{variable} must be set for the manual external PG17 compute/pgbench gate"
        );
    }
}

fn service_with_timeline(timeline: &TimelineKey) -> PageService<PostgresRedo<SyntheticRedoCodec>> {
    let mut store = InMemoryTimelineStore::new();
    store.create_timeline(timeline);
    PageService::new(store, PostgresRedo::new(SyntheticRedoCodec))
}

fn timeline_key(tenant_id: &str, timeline_id: &str) -> TimelineKey {
    TimelineKey::parse(
        "main".to_owned(),
        tenant_id.to_owned(),
        timeline_id.to_owned(),
    )
    .expect("test timeline parses")
}

fn page_key() -> PageKey {
    PageKey {
        rel: RelTag {
            spc_node: 1663,
            db_node: 5,
            rel_node: 42,
            fork_number: ForkNumber(0),
        },
        block_number: crabka_page_store::BlockNumber(0),
    }
}

fn page_image(seed: u8) -> Vec<u8> {
    let mut page = vec![0; PAGE_SIZE];
    page[0] = seed;
    page
}

fn borrowed_bytes(value: &str) -> ffi::CrabkaComputeBorrowedBytes {
    ffi::CrabkaComputeBorrowedBytes {
        ptr: value.as_ptr().cast(),
        len: value.len(),
    }
}

async fn serve_pageserver(service: Arc<PageService<PostgresRedo<SyntheticRedoCodec>>>) -> String {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("test listener has address");
    let app = connect_router(service);
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("pageserver test router serves");
    });

    format!("http://{address}")
}

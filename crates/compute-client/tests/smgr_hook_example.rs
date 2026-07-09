use assert2::assert;
use crabka_compute_client::{
    BlockNumber, ComputeClientError, ComputePageServerClient, DatabaseOid, ForkName,
    LocalMockComputePageServerClient, Lsn, PageFetchRequest, RelFileNode, TablespaceOid, TenantId,
    TimelineId,
};

fn read_page_through_future_smgr_hook(
    client: &impl ComputePageServerClient,
    request: PageFetchRequest,
) -> Result<Vec<u8>, ComputeClientError> {
    client.fetch_page(request).map(|page| page.bytes)
}

#[test]
fn future_smgr_hook_example_shapes_a_page_fetch() -> Result<(), ComputeClientError> {
    let client = LocalMockComputePageServerClient::new();
    let request = PageFetchRequest {
        tenant_id: TenantId::try_from("tenant-a")?,
        timeline_id: TimelineId::try_from("timeline-b")?,
        tablespace_oid: TablespaceOid::new(1663),
        database_oid: DatabaseOid::new(5),
        relfilenode: RelFileNode::new(42),
        fork_name: ForkName::Main,
        block_number: BlockNumber::new(7),
        request_lsn: Lsn::new(512),
    };

    let page = read_page_through_future_smgr_hook(&client, request)?;

    assert!(page.len() == 8_192);
    assert!(
        client.recorded_requests()[0].shape.path
            == "/v1/tenants/tenant-a/timelines/timeline-b/pages/1663/5/42/main/7"
    );
    Ok(())
}

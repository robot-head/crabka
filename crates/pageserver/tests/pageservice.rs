use std::sync::Arc;

use assert2::assert;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey, RelMetaKey, RelMetaKind, SlruKind, SlruPageKey};
use crabka_pageserver::{
    BasebackupMetadataRequest, BasebackupMetadataResponse, BasebackupMetadataRpcRequest,
    BasebackupMetadataRpcResponse, BasebackupRelMetaMetadata, BasebackupSlruPageMetadata,
    CreateTimelineRpcRequest, CreateTimelineRpcResponse, GetPageRequest, GetPageResponse,
    GetPageRpcRequest, GetPageRpcResponse, InMemoryTimelineStore, LiveIngestRecord,
    PageServerErrorBody, PageServerRpcRouter, PageService, PageServiceError, PostgresRedo,
    PutPageImageRpcRequest, PutPageImageRpcResponse, PutWalRecordRpcRequest,
    PutWalRecordRpcResponse, RelMetaRequest, RelMetaResponse, RelationSizeRequest,
    RelationSizeResponse, RpcTimeline, SlruPageRequest, SlruPageResponse, SyntheticRedoCodec,
    TimelineKey, connect_router, ingest_live_records, pb, relation_file_seed_pages,
    seed_page_images,
};
use crabka_postgres_redo::RedoRecord;
use crabka_postgres_wal::Lsn;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use tower::ServiceExt;

fn page_key(block_number: u32) -> PageKey {
    PageKey::new(1663, 5, 16_384, 0, block_number)
}

fn page(fill: u8) -> Bytes {
    Bytes::from(vec![fill; PAGE_SIZE])
}

fn timeline(branch: &str, timeline: &str) -> TimelineKey {
    TimelineKey::parse(branch, "tenant", timeline).expect("timeline identifiers are valid")
}

fn service() -> PageService<PostgresRedo<SyntheticRedoCodec>> {
    PageService::new(
        InMemoryTimelineStore::new(),
        PostgresRedo::new(SyntheticRedoCodec),
    )
}

fn basebackup_contains_page(response: &BasebackupMetadataResponse, page: &Bytes) -> bool {
    response.tar.windows(PAGE_SIZE).any(|window| window == page)
}

fn rpc_timeline(branch: &str, timeline: &str) -> RpcTimeline {
    RpcTimeline {
        branch_id: branch.to_owned(),
        tenant_id: "tenant".to_owned(),
        timeline_id: timeline.to_owned(),
    }
}

fn pb_timeline(branch: &str, timeline: &str) -> pb::Timeline {
    pb::Timeline {
        branch_id: branch.to_owned(),
        tenant_id: "tenant".to_owned(),
        timeline_id: timeline.to_owned(),
    }
}

fn pb_rel_tag(key: PageKey) -> pb::RelTag {
    pb::RelTag {
        spc_node: key.rel.spc_node,
        db_node: key.rel.db_node,
        rel_node: key.rel.rel_node,
        fork_number: u32::from(key.rel.fork_number.0),
    }
}

fn pb_page_key(key: PageKey) -> pb::PageKey {
    pb::PageKey {
        rel: Some(pb_rel_tag(key)),
        block_number: key.block_number.0,
    }
}

async fn post_rpc<RequestBody, ResponseBody>(
    app: axum::Router,
    path: &str,
    request: &RequestBody,
) -> (StatusCode, ResponseBody)
where
    RequestBody: Serialize,
    ResponseBody: DeserializeOwned,
{
    let response = app
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(request).expect("RPC request serializes"),
                ))
                .expect("RPC request builds"),
        )
        .await
        .expect("RPC handler responds");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("RPC response body is readable");
    let response = serde_json::from_slice(&body).expect("RPC response deserializes");
    (status, response)
}

async fn post_connect<RequestBody, ResponseBody>(
    app: axum::Router,
    path: &str,
    request: &RequestBody,
) -> (StatusCode, ResponseBody)
where
    RequestBody: Message,
    ResponseBody: Message + Default,
{
    let body = request.encode_to_vec();
    let response = app
        .oneshot(
            Request::post(path)
                .header("content-type", "application/proto")
                .header("connect-protocol-version", "1")
                .body(Body::from(body))
                .expect("Connect request builds"),
        )
        .await
        .expect("Connect handler responds");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Connect response body is readable");
    let response = ResponseBody::decode(body).expect("Connect response decodes as protobuf");
    (status, response)
}

#[tokio::test]
async fn missing_page_returns_not_found() {
    let service = service();
    let timeline = timeline("main", "timeline");
    service.store_mut().await.create_timeline(&timeline);

    let result = service
        .get_page(GetPageRequest {
            timeline,
            key: page_key(0),
            lsn: Lsn(10),
        })
        .await;

    assert!(matches!(result, Err(PageServiceError::PageNotFound { .. })));
}

#[tokio::test]
async fn missing_timeline_returns_not_found_before_reconstruction() {
    let service = service();

    let result = service
        .get_page(GetPageRequest {
            timeline: timeline("main", "missing"),
            key: page_key(0),
            lsn: Lsn(10),
        })
        .await;

    assert!(matches!(
        result,
        Err(PageServiceError::TimelineNotFound { .. })
    ));
}

#[tokio::test]
async fn image_lookup_returns_image_without_wal() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let image = page(7);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_image(&timeline, page_key(0), Lsn(10), image.clone())
            .await
            .expect("image layer writes");
    }

    let response = service
        .get_page(GetPageRequest {
            timeline,
            key: page_key(0),
            lsn: Lsn(10),
        })
        .await
        .expect("image reconstructs");

    assert!(response == GetPageResponse { page: image });
}

#[tokio::test]
async fn wal_redo_reconstructs_from_existing_synthetic_types() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let key = page_key(0);
    let base = page(0);
    let record = RedoRecord::byte_range_patch(key, Lsn(20), 4, Bytes::from_static(b"redo"));
    let encoded_record = SyntheticRedoCodec::encode_record(&record);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_image(&timeline, key, Lsn(10), base)
            .await
            .expect("base image layer writes");
        store
            .put_wal(&timeline, key, Lsn(20), false, encoded_record)
            .await
            .expect("delta layer writes");
    }

    let response = service
        .get_page(GetPageRequest {
            timeline,
            key,
            lsn: Lsn(20),
        })
        .await
        .expect("redo reconstructs");

    let reconstructed = response.page;
    assert!(&reconstructed[0..4] == [0, 0, 0, 0]);
    assert!(&reconstructed[4..8] == b"redo");
    assert!(reconstructed[8] == 0);
}

#[tokio::test]
async fn branch_and_timeline_namespaces_are_separate() {
    let service = service();
    let main_timeline = timeline("main", "timeline");
    let branch_timeline = timeline("branch", "timeline");
    let other_timeline = timeline("main", "other");
    let key = page_key(0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&main_timeline);
        store.create_timeline(&branch_timeline);
        store.create_timeline(&other_timeline);
        store
            .put_image(&main_timeline, key, Lsn(10), page(1))
            .await
            .expect("main image writes");
        store
            .put_image(&branch_timeline, key, Lsn(10), page(2))
            .await
            .expect("branch image writes");
        store
            .put_image(&other_timeline, key, Lsn(10), page(3))
            .await
            .expect("other timeline image writes");
    }

    let main_page = service
        .get_page(GetPageRequest {
            timeline: main_timeline,
            key,
            lsn: Lsn(10),
        })
        .await
        .expect("main page reconstructs")
        .page;
    let branch_page = service
        .get_page(GetPageRequest {
            timeline: branch_timeline,
            key,
            lsn: Lsn(10),
        })
        .await
        .expect("branch page reconstructs")
        .page;
    let other_page = service
        .get_page(GetPageRequest {
            timeline: other_timeline,
            key,
            lsn: Lsn(10),
        })
        .await
        .expect("other page reconstructs")
        .page;

    assert!(main_page == page(1));
    assert!(branch_page == page(2));
    assert!(other_page == page(3));
}

#[tokio::test]
async fn branch_reads_inherit_ancestor_at_boundary_and_enforce_child_writes_after_boundary() {
    let service = service();
    let parent = timeline("main", "parent");
    let child = timeline("child", "child");
    let key = page_key(0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&parent);
        store
            .put_image(&parent, key, Lsn(10), page(1))
            .await
            .expect("parent image writes");
        store
            .put_relation_size(&parent, key.rel, Lsn(20), 4)
            .expect("parent relation size writes");
        store
            .create_branch(&child, &parent, Lsn(20))
            .expect("branch creates");
    }

    let boundary_page = service
        .get_page(GetPageRequest {
            timeline: child.clone(),
            key,
            lsn: Lsn(20),
        })
        .await
        .expect("child inherits boundary page")
        .page;
    let inherited_size = service
        .relation_size(RelationSizeRequest {
            timeline: child.clone(),
            rel: key.rel,
            lsn: Lsn(20),
        })
        .await
        .expect("child inherits relation size");
    let boundary_write = service
        .store_mut()
        .await
        .put_image(&child, key, Lsn(20), page(2))
        .await;

    assert!(boundary_page == page(1));
    assert!(inherited_size == RelationSizeResponse { blocks: 4 });
    assert!(matches!(
        boundary_write,
        Err(PageServiceError::BranchBoundaryViolation { .. })
    ));
}

#[tokio::test]
async fn branch_delete_refuses_parent_until_leaf_is_deleted() {
    let service = service();
    let parent = timeline("main", "parent");
    let child = timeline("child", "child");
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&parent);
        store
            .put_image(&parent, page_key(0), Lsn(10), page(1))
            .await
            .expect("parent image writes");
        store
            .create_branch(&child, &parent, Lsn(10))
            .expect("branch creates");
    }

    let parent_delete = service.delete_timeline(&parent).await;
    let child_delete = service.delete_timeline(&child).await.expect("leaf deletes");
    let parent_delete_after_leaf = service
        .delete_timeline(&parent)
        .await
        .expect("parent deletes after child");

    assert!(matches!(
        parent_delete,
        Err(PageServiceError::TimelineHasDescendants { .. })
    ));
    assert!(child_delete.deleted);
    assert!(parent_delete_after_leaf.deleted);
}

#[tokio::test]
async fn branch_ancestry_uses_full_branch_namespace_for_shared_timeline_ids() {
    let service = service();
    let main_parent = timeline("main", "shared");
    let alternate_parent = timeline("alternate", "shared");
    let child = timeline("child", "child");
    let key = page_key(0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&main_parent);
        store.create_timeline(&alternate_parent);
        store
            .put_image(&main_parent, key, Lsn(10), page(1))
            .await
            .expect("main parent image writes");
        store
            .put_image(&alternate_parent, key, Lsn(10), page(2))
            .await
            .expect("alternate parent image writes");
        store
            .create_branch(&child, &alternate_parent, Lsn(10))
            .expect("branch creates from alternate parent");
    }

    let inherited_page = service
        .get_page(GetPageRequest {
            timeline: child.clone(),
            key,
            lsn: Lsn(10),
        })
        .await
        .expect("child inherits from selected parent")
        .page;
    let unrelated_delete = service
        .delete_timeline(&main_parent)
        .await
        .expect("unrelated same-id namespace deletes");
    let selected_parent_delete = service.delete_timeline(&alternate_parent).await;

    assert!(inherited_page == page(2));
    assert!(unrelated_delete.deleted);
    assert!(matches!(
        selected_parent_delete,
        Err(PageServiceError::TimelineHasDescendants { .. })
    ));
}

#[tokio::test]
async fn relation_size_returns_exact_metadata_for_existing_timeline() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let key = page_key(0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_relation_size(&timeline, key.rel, Lsn(10), 7)
            .expect("relation size metadata writes");
        store
            .put_relation_size(&timeline, key.rel, Lsn(20), 9)
            .expect("relation size metadata update writes");
    }

    let before_update = service
        .relation_size(RelationSizeRequest {
            timeline: timeline.clone(),
            rel: key.rel,
            lsn: Lsn(19),
        })
        .await
        .expect("relation size is available before update");
    let after_update = service
        .relation_size(RelationSizeRequest {
            timeline,
            rel: key.rel,
            lsn: Lsn(20),
        })
        .await
        .expect("relation size is available after update");

    assert!(before_update == RelationSizeResponse { blocks: 7 });
    assert!(after_update == RelationSizeResponse { blocks: 9 });
}

#[tokio::test]
async fn relmeta_and_slru_pages_return_exact_metadata() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let relmeta_key = RelMetaKey::new(page_key(0).rel, RelMetaKind::RelMap);
    let slru_key = SlruPageKey::new(SlruKind::Clog, 0, 1);
    let slru_page = page(11);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_relmeta(
                &timeline,
                relmeta_key,
                Lsn(10),
                Bytes::from_static(b"relmap"),
            )
            .expect("relation metadata writes");
        store
            .put_slru_page(&timeline, slru_key, Lsn(10), slru_page.clone())
            .expect("SLRU page writes");
    }

    let relmeta = service
        .relmeta(RelMetaRequest {
            timeline: timeline.clone(),
            key: relmeta_key,
            lsn: Lsn(10),
        })
        .await
        .expect("relation metadata is available");
    let slru = service
        .slru_page(SlruPageRequest {
            timeline,
            key: slru_key,
            lsn: Lsn(10),
        })
        .await
        .expect("SLRU page is available");

    assert!(
        relmeta
            == RelMetaResponse {
                metadata: Bytes::from_static(b"relmap")
            }
    );
    assert!(slru == SlruPageResponse { page: slru_page });
}

#[tokio::test]
async fn metadata_lookups_return_missing_errors_for_existing_timeline() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let key = page_key(0);
    let relmeta_key = RelMetaKey::new(key.rel, RelMetaKind::StorageManager);
    let slru_key = SlruPageKey::new(SlruKind::Clog, 0, 1);
    service.store_mut().await.create_timeline(&timeline);

    let relation_size = service
        .relation_size(RelationSizeRequest {
            timeline: timeline.clone(),
            rel: key.rel,
            lsn: Lsn(10),
        })
        .await;
    let relmeta = service
        .relmeta(RelMetaRequest {
            timeline: timeline.clone(),
            key: relmeta_key,
            lsn: Lsn(10),
        })
        .await;
    let slru = service
        .slru_page(SlruPageRequest {
            timeline,
            key: slru_key,
            lsn: Lsn(10),
        })
        .await;

    assert!(matches!(
        relation_size,
        Err(PageServiceError::RelationSizeMissing { .. })
    ));
    assert!(matches!(
        relmeta,
        Err(PageServiceError::RelMetaMissing { .. })
    ));
    assert!(matches!(
        slru,
        Err(PageServiceError::SlruPageMissing { .. })
    ));
}

#[tokio::test]
async fn metadata_namespaces_are_isolated_by_branch_and_timeline() {
    let service = service();
    let main_timeline = timeline("main", "timeline");
    let branch_timeline = timeline("branch", "timeline");
    let other_timeline = timeline("main", "other");
    let key = page_key(0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&main_timeline);
        store.create_timeline(&branch_timeline);
        store.create_timeline(&other_timeline);
        store
            .put_relation_size(&main_timeline, key.rel, Lsn(10), 1)
            .expect("main relation size metadata writes");
        store
            .put_relation_size(&branch_timeline, key.rel, Lsn(10), 2)
            .expect("branch relation size metadata writes");
        store
            .put_relation_size(&other_timeline, key.rel, Lsn(10), 3)
            .expect("other relation size metadata writes");
    }

    let main_size = service
        .relation_size(RelationSizeRequest {
            timeline: main_timeline,
            rel: key.rel,
            lsn: Lsn(10),
        })
        .await
        .expect("main relation size is available");
    let branch_size = service
        .relation_size(RelationSizeRequest {
            timeline: branch_timeline,
            rel: key.rel,
            lsn: Lsn(10),
        })
        .await
        .expect("branch relation size is available");
    let other_size = service
        .relation_size(RelationSizeRequest {
            timeline: other_timeline,
            rel: key.rel,
            lsn: Lsn(10),
        })
        .await
        .expect("other relation size is available");

    assert!(main_size == RelationSizeResponse { blocks: 1 });
    assert!(branch_size == RelationSizeResponse { blocks: 2 });
    assert!(other_size == RelationSizeResponse { blocks: 3 });
}

#[tokio::test]
async fn basebackup_metadata_reports_empty_snapshot_for_existing_timeline() {
    let service = service();
    let timeline = timeline("main", "timeline");
    service.store_mut().await.create_timeline(&timeline);

    let response = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: timeline.clone(),
            lsn: Lsn(10),
        })
        .await
        .expect("basebackup metadata exists for an empty timeline");

    assert!(
        response
            == BasebackupMetadataResponse {
                timeline,
                lsn: Lsn(10),
                relmeta: Vec::new(),
                slru_pages: Vec::new(),
                tar: response.tar.clone(),
            }
    );
    assert!(response.tar.starts_with(b"CRABKA_BASEBACKUP_V1\n"));
}

#[tokio::test]
async fn basebackup_payload_contains_reconstructed_pages_and_metadata() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let key = page_key(0);
    let image = page(77);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_image(&timeline, key, Lsn(10), image.clone())
            .await
            .expect("image writes");
        store
            .put_relation_size(&timeline, key.rel, Lsn(10), 1)
            .expect("relation size writes");
    }

    let response = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline,
            lsn: Lsn(10),
        })
        .await
        .expect("basebackup payload builds");

    assert!(response.tar.starts_with(b"CRABKA_BASEBACKUP_V1\n"));
    assert!(
        response
            .tar
            .windows(PAGE_SIZE)
            .any(|window| window == image)
    );
    assert!(
        response
            .tar
            .windows(4)
            .any(|window| window == 1_u32.to_le_bytes())
    );
}

#[tokio::test]
async fn branch_basebackup_before_branch_point_uses_requested_ancestor_lsn() {
    let service = service();
    let parent = timeline("main", "parent");
    let child = timeline("child", "child");
    let early_key = page_key(0);
    let later_key = page_key(1);
    let early_page = page(11);
    let later_page = page(22);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&parent);
        store
            .put_image(&parent, early_key, Lsn(10), early_page.clone())
            .await
            .expect("early parent image writes");
        store
            .put_image(&parent, later_key, Lsn(20), later_page.clone())
            .await
            .expect("later parent image writes");
        store
            .put_relation_size(&parent, early_key.rel, Lsn(30), 2)
            .expect("parent relation size advances head");
        store
            .create_branch(&child, &parent, Lsn(30))
            .expect("branch creates");
    }

    let response = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: child,
            lsn: Lsn(10),
        })
        .await
        .expect("branch basebackup builds at older LSN");

    assert!(basebackup_contains_page(&response, &early_page));
    assert!(!basebackup_contains_page(&response, &later_page));
}

#[tokio::test]
async fn branch_basebackup_at_and_after_branch_point_includes_inherited_and_child_pages() {
    let service = service();
    let parent = timeline("main", "parent");
    let child = timeline("child", "child");
    let overwritten_key = page_key(0);
    let inherited_key = page_key(1);
    let child_key = page_key(2);
    let original_parent_page = page(33);
    let inherited_parent_page = page(44);
    let child_overwrite_page = page(55);
    let child_new_page = page(66);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&parent);
        store
            .put_image(
                &parent,
                overwritten_key,
                Lsn(10),
                original_parent_page.clone(),
            )
            .await
            .expect("first parent image writes");
        store
            .put_image(
                &parent,
                inherited_key,
                Lsn(20),
                inherited_parent_page.clone(),
            )
            .await
            .expect("second parent image writes");
        store
            .put_relation_size(&parent, overwritten_key.rel, Lsn(30), 2)
            .expect("parent relation size advances head");
        store
            .create_branch(&child, &parent, Lsn(30))
            .expect("branch creates");
        store
            .put_image(
                &child,
                overwritten_key,
                Lsn(40),
                child_overwrite_page.clone(),
            )
            .await
            .expect("child overwrite writes");
        store
            .put_image(&child, child_key, Lsn(40), child_new_page.clone())
            .await
            .expect("child new page writes");
    }

    let at_branch_point = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: child.clone(),
            lsn: Lsn(30),
        })
        .await
        .expect("branch basebackup builds at branch point");
    let after_branch_point = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: child,
            lsn: Lsn(40),
        })
        .await
        .expect("branch basebackup builds after branch point");

    assert!(basebackup_contains_page(
        &at_branch_point,
        &original_parent_page
    ));
    assert!(basebackup_contains_page(
        &at_branch_point,
        &inherited_parent_page
    ));
    assert!(!basebackup_contains_page(
        &at_branch_point,
        &child_overwrite_page
    ));
    assert!(!basebackup_contains_page(&at_branch_point, &child_new_page));
    assert!(!basebackup_contains_page(
        &after_branch_point,
        &original_parent_page
    ));
    assert!(basebackup_contains_page(
        &after_branch_point,
        &inherited_parent_page
    ));
    assert!(basebackup_contains_page(
        &after_branch_point,
        &child_overwrite_page
    ));
    assert!(basebackup_contains_page(
        &after_branch_point,
        &child_new_page
    ));
}

#[tokio::test]
async fn seed_and_live_ingest_seams_reconstruct_through_service() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let key = page_key(0);
    let base_file = page(0);
    let record = RedoRecord::byte_range_patch(key, Lsn(20), 4, Bytes::from_static(b"live"));
    {
        let mut store = service.store_mut().await;
        seed_page_images(
            &mut store,
            &timeline,
            Lsn(10),
            relation_file_seed_pages(key, &base_file),
        )
        .await
        .expect("seed pages write");
        ingest_live_records(
            &mut store,
            &timeline,
            [LiveIngestRecord {
                key,
                lsn: Lsn(20),
                will_init: false,
                record: SyntheticRedoCodec::encode_record(&record),
            }],
        )
        .await
        .expect("live records ingest");
    }

    let response = service
        .get_page(GetPageRequest {
            timeline,
            key,
            lsn: Lsn(20),
        })
        .await
        .expect("live record reconstructs");

    assert!(&response.page[4..8] == b"live");
}

#[tokio::test]
async fn basebackup_metadata_includes_visible_slru_and_relmeta_in_order() {
    let service = service();
    let timeline = timeline("main", "timeline");
    let rel_a = RelMetaKey::new(page_key(0).rel, RelMetaKind::Size);
    let rel_b = RelMetaKey::new(page_key(1).rel, RelMetaKind::RelMap);
    let clog = SlruPageKey::new(SlruKind::Clog, 0, 1);
    let multixact = SlruPageKey::new(SlruKind::MultiXactMember, 0, 0);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_relmeta(&timeline, rel_b, Lsn(10), Bytes::from_static(b"rel-b"))
            .expect("relation metadata writes");
        store
            .put_relation_size(&timeline, rel_a.rel, Lsn(10), 8)
            .expect("relation-size metadata writes");
        store
            .put_slru_page(&timeline, multixact, Lsn(12), page(7))
            .expect("SLRU page writes");
        store
            .put_slru_page(&timeline, clog, Lsn(10), page(3))
            .expect("SLRU page writes");
        store
            .put_slru_page(&timeline, clog, Lsn(20), page(9))
            .expect("newer SLRU page writes");
    }

    let response = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: timeline.clone(),
            lsn: Lsn(12),
        })
        .await
        .expect("basebackup metadata is available");

    assert!(
        response
            == BasebackupMetadataResponse {
                timeline,
                lsn: Lsn(12),
                relmeta: vec![
                    BasebackupRelMetaMetadata {
                        key: rel_a,
                        metadata: Bytes::copy_from_slice(&8_u32.to_le_bytes()),
                    },
                    BasebackupRelMetaMetadata {
                        key: rel_b,
                        metadata: Bytes::from_static(b"rel-b"),
                    },
                ],
                slru_pages: vec![
                    BasebackupSlruPageMetadata {
                        key: clog,
                        page: page(3),
                    },
                    BasebackupSlruPageMetadata {
                        key: multixact,
                        page: page(7),
                    },
                ],
                tar: response.tar.clone(),
            }
    );
    assert!(response.tar.starts_with(b"CRABKA_BASEBACKUP_V1\n"));
    assert!(
        response
            .tar
            .windows(b"rel-b".len())
            .any(|window| window == b"rel-b")
    );
}

#[tokio::test]
async fn slru_and_relmeta_basebackup_metadata_are_branch_and_timeline_isolated() {
    let service = service();
    let main_timeline = timeline("main", "timeline");
    let branch_timeline = timeline("branch", "timeline");
    let other_timeline = timeline("main", "other");
    let relmeta = RelMetaKey::new(page_key(0).rel, RelMetaKind::StorageManager);
    let slru = SlruPageKey::new(SlruKind::Clog, 0, 1);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&main_timeline);
        store.create_timeline(&branch_timeline);
        store.create_timeline(&other_timeline);
        store
            .put_relmeta(
                &main_timeline,
                relmeta,
                Lsn(10),
                Bytes::from_static(b"main"),
            )
            .expect("main relation metadata writes");
        store
            .put_relmeta(
                &branch_timeline,
                relmeta,
                Lsn(10),
                Bytes::from_static(b"branch"),
            )
            .expect("branch relation metadata writes");
        store
            .put_relmeta(
                &other_timeline,
                relmeta,
                Lsn(10),
                Bytes::from_static(b"other"),
            )
            .expect("other relation metadata writes");
        store
            .put_slru_page(&main_timeline, slru, Lsn(10), page(1))
            .expect("main SLRU writes");
        store
            .put_slru_page(&branch_timeline, slru, Lsn(10), page(2))
            .expect("branch SLRU writes");
        store
            .put_slru_page(&other_timeline, slru, Lsn(10), page(3))
            .expect("other SLRU writes");
    }

    let main = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: main_timeline,
            lsn: Lsn(10),
        })
        .await
        .expect("main metadata is available");
    let branch = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: branch_timeline,
            lsn: Lsn(10),
        })
        .await
        .expect("branch metadata is available");
    let other = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: other_timeline,
            lsn: Lsn(10),
        })
        .await
        .expect("other metadata is available");

    assert!(main.relmeta[0].metadata == Bytes::from_static(b"main"));
    assert!(branch.relmeta[0].metadata == Bytes::from_static(b"branch"));
    assert!(other.relmeta[0].metadata == Bytes::from_static(b"other"));
    assert!(main.slru_pages[0].page == page(1));
    assert!(branch.slru_pages[0].page == page(2));
    assert!(other.slru_pages[0].page == page(3));
}

#[tokio::test]
async fn metadata_requests_still_fail_fast_for_missing_timeline() {
    let service = service();
    let key = page_key(0);

    let relation_size = service
        .relation_size(RelationSizeRequest {
            timeline: timeline("main", "missing"),
            rel: key.rel,
            lsn: Lsn(10),
        })
        .await;
    let relmeta = service
        .relmeta(RelMetaRequest {
            timeline: timeline("main", "missing"),
            key: RelMetaKey::new(key.rel, RelMetaKind::RelMap),
            lsn: Lsn(10),
        })
        .await;
    let slru = service
        .slru_page(SlruPageRequest {
            timeline: timeline("main", "missing"),
            key: SlruPageKey::new(SlruKind::Clog, 0, 1),
            lsn: Lsn(10),
        })
        .await;
    let basebackup_metadata = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: timeline("main", "missing"),
            lsn: Lsn(10),
        })
        .await;

    assert!(matches!(
        relation_size,
        Err(PageServiceError::TimelineNotFound { .. })
    ));
    assert!(matches!(
        basebackup_metadata,
        Err(PageServiceError::TimelineNotFound { .. })
    ));
    assert!(matches!(
        relmeta,
        Err(PageServiceError::TimelineNotFound { .. })
    ));
    assert!(matches!(
        slru,
        Err(PageServiceError::TimelineNotFound { .. })
    ));
}

#[tokio::test]
async fn rpc_create_timeline_and_page_fetch_round_trip_through_router() {
    let service = Arc::new(service());
    let app = PageServerRpcRouter::router(Arc::clone(&service));
    let timeline = rpc_timeline("main", "timeline");
    let key = page_key(0);
    let image = page(42);

    let (create_status, create): (StatusCode, CreateTimelineRpcResponse) = post_rpc(
        app.clone(),
        "/v1/pageserver/timeline:create",
        &CreateTimelineRpcRequest {
            timeline: timeline.clone(),
        },
    )
    .await;
    let (put_status, put): (StatusCode, PutPageImageRpcResponse) = post_rpc(
        app.clone(),
        "/v1/pageserver/page:put-image",
        &PutPageImageRpcRequest {
            timeline: timeline.clone(),
            key,
            lsn: 10,
            page: image.to_vec(),
        },
    )
    .await;
    let (get_status, get): (StatusCode, GetPageRpcResponse) = post_rpc(
        app,
        "/v1/pageserver/page:get",
        &GetPageRpcRequest {
            timeline: timeline.clone(),
            key,
            lsn: 10,
        },
    )
    .await;

    assert!(create_status == StatusCode::OK);
    assert!(put_status == StatusCode::OK);
    assert!(get_status == StatusCode::OK);
    assert!(
        create
            == CreateTimelineRpcResponse {
                timeline: timeline.clone(),
                created: true
            }
    );
    assert!(
        put == PutPageImageRpcResponse {
            timeline,
            key,
            lsn: 10
        }
    );
    assert!(
        get == GetPageRpcResponse {
            page: image.to_vec()
        }
    );
}

#[tokio::test]
async fn rpc_wal_ingest_reconstructs_page_through_router() {
    let service = Arc::new(service());
    let app = PageServerRpcRouter::router(Arc::clone(&service));
    let timeline = rpc_timeline("main", "timeline");
    let key = page_key(0);
    let record = RedoRecord::byte_range_patch(key, Lsn(20), 4, Bytes::from_static(b"redo"));

    let (create_status, _create): (StatusCode, CreateTimelineRpcResponse) = post_rpc(
        app.clone(),
        "/v1/pageserver/timeline:create",
        &CreateTimelineRpcRequest {
            timeline: timeline.clone(),
        },
    )
    .await;
    let (put_image_status, _put_image): (StatusCode, PutPageImageRpcResponse) = post_rpc(
        app.clone(),
        "/v1/pageserver/page:put-image",
        &PutPageImageRpcRequest {
            timeline: timeline.clone(),
            key,
            lsn: 10,
            page: page(0).to_vec(),
        },
    )
    .await;
    let (put_wal_status, put_wal): (StatusCode, PutWalRecordRpcResponse) = post_rpc(
        app.clone(),
        "/v1/pageserver/page:put-wal",
        &PutWalRecordRpcRequest {
            timeline: timeline.clone(),
            key,
            lsn: 20,
            will_init: false,
            record: SyntheticRedoCodec::encode_record(&record).to_vec(),
        },
    )
    .await;
    let (get_status, get): (StatusCode, GetPageRpcResponse) = post_rpc(
        app,
        "/v1/pageserver/page:get",
        &GetPageRpcRequest {
            timeline: timeline.clone(),
            key,
            lsn: 20,
        },
    )
    .await;

    assert!(create_status == StatusCode::OK);
    assert!(put_image_status == StatusCode::OK);
    assert!(put_wal_status == StatusCode::OK);
    assert!(get_status == StatusCode::OK);
    assert!(
        put_wal
            == PutWalRecordRpcResponse {
                timeline,
                key,
                lsn: 20,
                will_init: false,
            }
    );
    assert!(&get.page[4..8] == b"redo");
}

#[tokio::test]
async fn rpc_basebackup_metadata_returns_visible_metadata() {
    let service = Arc::new(service());
    let app = PageServerRpcRouter::router(Arc::clone(&service));
    let timeline = timeline("main", "timeline");
    let rpc_timeline = rpc_timeline("main", "timeline");
    let relmeta = RelMetaKey::new(page_key(0).rel, RelMetaKind::RelMap);
    let slru = SlruPageKey::new(SlruKind::Clog, 0, 1);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&timeline);
        store
            .put_relmeta(&timeline, relmeta, Lsn(10), Bytes::from_static(b"metadata"))
            .expect("relation metadata writes");
        store
            .put_slru_page(&timeline, slru, Lsn(10), page(6))
            .expect("SLRU page writes");
    }

    let (status, response): (StatusCode, BasebackupMetadataRpcResponse) = post_rpc(
        app,
        "/v1/pageserver/basebackup/metadata:get",
        &BasebackupMetadataRpcRequest {
            timeline: rpc_timeline.clone(),
            lsn: 10,
        },
    )
    .await;

    assert!(status == StatusCode::OK);
    assert!(response.timeline == rpc_timeline);
    assert!(response.lsn == 10);
    assert!(response.relmeta[0].key == relmeta);
    assert!(response.relmeta[0].metadata == b"metadata");
    assert!(response.slru_pages[0].key == slru);
    assert!(response.slru_pages[0].page == page(6).to_vec());
}

#[tokio::test]
async fn rpc_errors_map_service_failures_to_status_and_code() {
    let app = PageServerRpcRouter::router(Arc::new(service()));

    let (status, error): (StatusCode, PageServerErrorBody) = post_rpc(
        app,
        "/v1/pageserver/page:get",
        &GetPageRpcRequest {
            timeline: rpc_timeline("main", "missing"),
            key: page_key(0),
            lsn: 10,
        },
    )
    .await;

    assert!(status == StatusCode::NOT_FOUND);
    assert!(error.code == "timeline_not_found");
}

#[tokio::test]
async fn rpc_errors_map_boundary_parse_failures_to_bad_request() {
    let app = PageServerRpcRouter::router(Arc::new(service()));

    let (status, error): (StatusCode, PageServerErrorBody) = post_rpc(
        app,
        "/v1/pageserver/timeline:create",
        &CreateTimelineRpcRequest {
            timeline: RpcTimeline {
                branch_id: "bad/branch".to_owned(),
                tenant_id: "tenant".to_owned(),
                timeline_id: "timeline".to_owned(),
            },
        },
    )
    .await;

    assert!(status == StatusCode::BAD_REQUEST);
    assert!(error.code == "bad_request");
}

#[tokio::test]
async fn connect_seed_get_rel_size_and_basebackup_use_protobuf_surface() {
    let service = Arc::new(service());
    let app = connect_router(Arc::clone(&service));
    let pb_tl = pb_timeline("main", "timeline");
    let key = page_key(0);
    let image = page(55);

    let (create_status, create): (StatusCode, pb::CreateBranchResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/CreateBranch",
        &pb::CreateBranchRequest {
            timeline: Some(pb_tl.clone()),
            source_timeline: Some(pb_tl.clone()),
            branch_lsn: 10,
        },
    )
    .await;
    {
        let typed_timeline = timeline("main", "timeline");
        service
            .store_mut()
            .await
            .put_relation_size(&typed_timeline, key.rel, Lsn(10), 1)
            .expect("relation size metadata writes");
    }
    let (seed_status, seed): (StatusCode, pb::SeedImageResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/SeedImage",
        &pb::SeedImageRequest {
            timeline: Some(pb_tl.clone()),
            key: Some(pb_page_key(key)),
            lsn: 10,
            page: image.to_vec(),
        },
    )
    .await;
    let (get_status, get): (StatusCode, pb::GetPageResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/GetPage",
        &pb::GetPageRequest {
            timeline: Some(pb_tl.clone()),
            key: Some(pb_page_key(key)),
            lsn: 10,
        },
    )
    .await;
    let (size_status, size): (StatusCode, pb::GetRelSizeResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/GetRelSize",
        &pb::GetRelSizeRequest {
            timeline: Some(pb_tl.clone()),
            rel: Some(pb_rel_tag(key)),
            lsn: 10,
        },
    )
    .await;
    let (basebackup_status, basebackup): (StatusCode, pb::BasebackupResponse) = post_connect(
        app,
        "/crabka.pageserver.v1.PageService/Basebackup",
        &pb::BasebackupRequest {
            timeline: Some(pb_tl.clone()),
            lsn: 10,
        },
    )
    .await;

    assert!(create_status == StatusCode::OK);
    assert!(seed_status == StatusCode::OK);
    assert!(get_status == StatusCode::OK);
    assert!(size_status == StatusCode::OK);
    assert!(basebackup_status == StatusCode::OK);
    assert!(create.created);
    assert!(create.timeline == Some(pb_tl.clone()));
    assert!(seed.key == Some(pb_page_key(key)));
    assert!(get.page == image.to_vec());
    assert!(size.blocks == 1);
    assert!(basebackup.timeline == Some(pb_tl));
    assert!(basebackup.relmeta.len() == 1);
    assert!(!basebackup.tar.is_empty());
    assert!(basebackup.tar.starts_with(b"CRABKA_BASEBACKUP_V1\n"));
}

#[tokio::test]
async fn connect_ingest_wal_and_timeline_operations_map_to_service_backend() {
    let service = Arc::new(service());
    let app = connect_router(Arc::clone(&service));
    let timeline = pb_timeline("main", "timeline");
    let key = page_key(0);
    let record = RedoRecord::byte_range_patch(key, Lsn(20), 4, Bytes::from_static(b"redo"));

    let (_create_status, _create): (StatusCode, pb::CreateBranchResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/CreateBranch",
        &pb::CreateBranchRequest {
            timeline: Some(timeline.clone()),
            source_timeline: Some(timeline.clone()),
            branch_lsn: 10,
        },
    )
    .await;
    let (_seed_status, _seed): (StatusCode, pb::SeedImageResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/SeedImage",
        &pb::SeedImageRequest {
            timeline: Some(timeline.clone()),
            key: Some(pb_page_key(key)),
            lsn: 10,
            page: page(0).to_vec(),
        },
    )
    .await;
    let (ingest_status, ingest): (StatusCode, pb::IngestWalResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/IngestWal",
        &pb::IngestWalRequest {
            timeline: Some(timeline.clone()),
            key: Some(pb_page_key(key)),
            lsn: 20,
            will_init: false,
            record: SyntheticRedoCodec::encode_record(&record).to_vec(),
        },
    )
    .await;
    let (get_status, get): (StatusCode, pb::GetPageResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/GetPage",
        &pb::GetPageRequest {
            timeline: Some(timeline.clone()),
            key: Some(pb_page_key(key)),
            lsn: 20,
        },
    )
    .await;
    let (list_status, list): (StatusCode, pb::ListTimelinesResponse) = post_connect(
        app.clone(),
        "/crabka.pageserver.v1.PageService/ListTimelines",
        &pb::ListTimelinesRequest {
            tenant_id: "tenant".to_owned(),
        },
    )
    .await;
    let (delete_status, deleted): (StatusCode, pb::DeleteTimelineResponse) = post_connect(
        app,
        "/crabka.pageserver.v1.PageService/DeleteTimeline",
        &pb::DeleteTimelineRequest {
            timeline: Some(timeline.clone()),
        },
    )
    .await;

    assert!(ingest_status == StatusCode::OK);
    assert!(get_status == StatusCode::OK);
    assert!(list_status == StatusCode::OK);
    assert!(delete_status == StatusCode::OK);
    assert!(ingest.key == Some(pb_page_key(key)));
    assert!(&get.page[4..8] == b"redo");
    assert!(list.timelines == vec![timeline.clone()]);
    assert!(deleted.timeline == Some(timeline));
    assert!(deleted.deleted);
}

#[tokio::test]
async fn connect_errors_use_connect_status_codes() {
    let app = connect_router(Arc::new(service()));

    let response = app
        .oneshot(
            Request::post("/crabka.pageserver.v1.PageService/GetPage")
                .header("content-type", "application/proto")
                .header("connect-protocol-version", "1")
                .body(Body::from(
                    pb::GetPageRequest {
                        timeline: Some(pb_timeline("main", "missing")),
                        key: Some(pb_page_key(page_key(0))),
                        lsn: 10,
                    }
                    .encode_to_vec(),
                ))
                .expect("Connect request builds"),
        )
        .await
        .expect("Connect handler responds");

    assert!(response.status() == StatusCode::NOT_FOUND);
}

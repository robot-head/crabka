//! Connect-RPC boundary for the pageserver service.

use std::sync::Arc;

use axum::Extension;
use bytes::Bytes;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse, error::Code};
use crabka_page_store::{
    BlockNumber, ForkNumber, PageKey, RelMetaKey, RelMetaKind, RelTag, SlruKind, SlruPageKey,
};
use crabka_postgres_wal::Lsn;

use crate::{
    BasebackupMetadataRequest, CreateBranchRequest, PageRedo, PageService, PageServiceError,
    PutPageImageRequest, PutWalRecordRequest, RelationSizeRequest, TimelineKey, pb,
};

/// Builds the plan-aligned Connect-RPC service surface.
pub(crate) fn router<R>(service: Arc<PageService<R>>) -> axum::Router
where
    R: PageRedo + Send + Sync + 'static,
{
    pb::page_service_connect::PageServiceBuilder::<()>::new()
        .get_page(get_page::<R>)
        .get_rel_size(get_rel_size::<R>)
        .basebackup(basebackup::<R>)
        .create_branch(create_branch::<R>)
        .list_timelines(list_timelines::<R>)
        .delete_timeline(delete_timeline::<R>)
        .seed_image(seed_image::<R>)
        .ingest_wal(ingest_wal::<R>)
        .build_connect()
        .layer(Extension(service))
}

pub(crate) async fn get_page<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::GetPageRequest>,
) -> Result<ConnectResponse<pb::GetPageResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .get_page(crate::GetPageRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            key: parse_page_key(request.key).map_err(invalid_argument)?,
            lsn: Lsn(request.lsn),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::GetPageResponse {
        page: response.page.to_vec(),
    }))
}

pub(crate) async fn get_rel_size<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::GetRelSizeRequest>,
) -> Result<ConnectResponse<pb::GetRelSizeResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .relation_size(RelationSizeRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            rel: parse_rel_tag(request.rel).map_err(invalid_argument)?,
            lsn: Lsn(request.lsn),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::GetRelSizeResponse {
        blocks: response.blocks,
    }))
}

pub(crate) async fn basebackup<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::BasebackupRequest>,
) -> Result<ConnectResponse<pb::BasebackupResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            lsn: Lsn(request.lsn),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;

    Ok(ConnectResponse(pb::BasebackupResponse {
        timeline: Some(timeline_to_pb(&response.timeline)),
        lsn: response.lsn.value(),
        relmeta: response
            .relmeta
            .into_iter()
            .map(|metadata| pb::BasebackupRelMeta {
                key: Some(relmeta_key_to_pb(metadata.key)),
                metadata: metadata.metadata.to_vec(),
            })
            .collect(),
        slru_pages: response
            .slru_pages
            .into_iter()
            .map(|metadata| pb::BasebackupSlruPage {
                key: Some(slru_key_to_pb(metadata.key)),
                page: metadata.page.to_vec(),
            })
            .collect(),
        tar: response.tar.to_vec(),
    }))
}

pub(crate) async fn create_branch<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::CreateBranchRequest>,
) -> Result<ConnectResponse<pb::CreateBranchResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .create_branch(CreateBranchRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            source_timeline: parse_timeline(request.source_timeline).map_err(invalid_argument)?,
            branch_lsn: Lsn(request.branch_lsn),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::CreateBranchResponse {
        timeline: Some(timeline_to_pb(&response.timeline)),
        created: response.created,
        source_timeline: Some(timeline_to_pb(&response.source_timeline)),
        branch_lsn: response.branch_lsn.value(),
    }))
}

pub(crate) async fn list_timelines<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::ListTimelinesRequest>,
) -> Result<ConnectResponse<pb::ListTimelinesResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let tenant_id = req.0.tenant_id;
    let timelines = service
        .list_timelines()
        .await
        .timelines
        .into_iter()
        .filter(|timeline| tenant_id.is_empty() || timeline.path.tenant_id.to_string() == tenant_id)
        .map(|timeline| timeline_to_pb(&timeline))
        .collect();
    Ok(ConnectResponse(pb::ListTimelinesResponse { timelines }))
}

pub(crate) async fn delete_timeline<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::DeleteTimelineRequest>,
) -> Result<ConnectResponse<pb::DeleteTimelineResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let timeline = parse_timeline(req.0.timeline).map_err(invalid_argument)?;
    let response = service
        .delete_timeline(&timeline)
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::DeleteTimelineResponse {
        timeline: Some(timeline_to_pb(&response.timeline)),
        deleted: response.deleted,
    }))
}

pub(crate) async fn seed_image<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::SeedImageRequest>,
) -> Result<ConnectResponse<pb::SeedImageResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .put_page_image(PutPageImageRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            key: parse_page_key(request.key).map_err(invalid_argument)?,
            lsn: Lsn(request.lsn),
            page: Bytes::from(request.page),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::SeedImageResponse {
        timeline: Some(timeline_to_pb(&response.timeline)),
        key: Some(page_key_to_pb(response.key)),
        lsn: response.lsn.value(),
    }))
}

pub(crate) async fn ingest_wal<R>(
    Extension(service): Extension<Arc<PageService<R>>>,
    req: ConnectRequest<pb::IngestWalRequest>,
) -> Result<ConnectResponse<pb::IngestWalResponse>, ConnectError>
where
    R: PageRedo + Send + Sync,
{
    let request = req.0;
    let response = service
        .put_wal_record(PutWalRecordRequest {
            timeline: parse_timeline(request.timeline).map_err(invalid_argument)?,
            key: parse_page_key(request.key).map_err(invalid_argument)?,
            lsn: Lsn(request.lsn),
            will_init: request.will_init,
            record: Bytes::from(request.record),
        })
        .await
        .map_err(|err| connect_error_from_service(&err))?;
    Ok(ConnectResponse(pb::IngestWalResponse {
        timeline: Some(timeline_to_pb(&response.timeline)),
        key: Some(page_key_to_pb(response.key)),
        lsn: response.lsn.value(),
        will_init: response.will_init,
    }))
}

fn parse_timeline(timeline: Option<pb::Timeline>) -> Result<TimelineKey, String> {
    let timeline = timeline.ok_or_else(|| "timeline is required".to_owned())?;
    TimelineKey::parse(timeline.branch_id, timeline.tenant_id, timeline.timeline_id)
        .map_err(|err| err.to_string())
}

fn parse_page_key(key: Option<pb::PageKey>) -> Result<PageKey, String> {
    let key = key.ok_or_else(|| "page key is required".to_owned())?;
    Ok(PageKey {
        rel: parse_rel_tag(key.rel)?,
        block_number: BlockNumber(key.block_number),
    })
}

fn parse_rel_tag(rel: Option<pb::RelTag>) -> Result<RelTag, String> {
    let rel = rel.ok_or_else(|| "relation tag is required".to_owned())?;
    let fork_number = u8::try_from(rel.fork_number)
        .map_err(|_| format!("fork number {} is too large", rel.fork_number))?;
    Ok(RelTag {
        spc_node: rel.spc_node,
        db_node: rel.db_node,
        rel_node: rel.rel_node,
        fork_number: ForkNumber(fork_number),
    })
}

fn connect_error_from_service(err: &PageServiceError) -> ConnectError {
    let code = match err {
        PageServiceError::TimelineNotFound { .. }
        | PageServiceError::PageNotFound { .. }
        | PageServiceError::RelationSizeMissing { .. }
        | PageServiceError::SlruPageMissing { .. }
        | PageServiceError::RelMetaMissing { .. }
        | PageServiceError::BranchSourceNotFound { .. } => Code::NotFound,
        PageServiceError::WrongImageSize { .. }
        | PageServiceError::WrongSlruPageSize { .. }
        | PageServiceError::BranchLsnBeyondHead { .. }
        | PageServiceError::BranchBoundaryViolation { .. }
        | PageServiceError::TimelineHasDescendants { .. } => Code::InvalidArgument,
        PageServiceError::PageStore(_) | PageServiceError::Redo(_) => Code::Internal,
    };
    ConnectError::new(code, err.to_string())
}

fn invalid_argument(message: impl Into<String>) -> ConnectError {
    ConnectError::new(Code::InvalidArgument, message.into())
}

fn timeline_to_pb(timeline: &TimelineKey) -> pb::Timeline {
    pb::Timeline {
        branch_id: timeline.branch_id.to_string(),
        tenant_id: timeline.path.tenant_id.to_string(),
        timeline_id: timeline.path.timeline_id.to_string(),
    }
}

fn page_key_to_pb(key: PageKey) -> pb::PageKey {
    pb::PageKey {
        rel: Some(rel_tag_to_pb(key.rel)),
        block_number: key.block_number.0,
    }
}

fn rel_tag_to_pb(rel: RelTag) -> pb::RelTag {
    pb::RelTag {
        spc_node: rel.spc_node,
        db_node: rel.db_node,
        rel_node: rel.rel_node,
        fork_number: u32::from(rel.fork_number.0),
    }
}

fn relmeta_key_to_pb(key: RelMetaKey) -> pb::RelMetaKey {
    pb::RelMetaKey {
        rel: Some(rel_tag_to_pb(key.rel)),
        kind: relmeta_kind_to_pb(key.kind).into(),
    }
}

fn relmeta_kind_to_pb(kind: RelMetaKind) -> pb::RelMetaKind {
    match kind {
        RelMetaKind::Size => pb::RelMetaKind::Size,
        RelMetaKind::RelMap => pb::RelMetaKind::RelMap,
        RelMetaKind::StorageManager => pb::RelMetaKind::StorageManager,
    }
}

fn slru_key_to_pb(key: SlruPageKey) -> pb::SlruPageKey {
    pb::SlruPageKey {
        kind: slru_kind_to_pb(key.kind).into(),
        segment_number: key.segment_number,
        block_number: key.block_number,
    }
}

fn slru_kind_to_pb(kind: SlruKind) -> pb::SlruKind {
    match kind {
        SlruKind::Clog => pb::SlruKind::Clog,
        SlruKind::MultiXactOffset => pb::SlruKind::MultiXactOffset,
        SlruKind::MultiXactMember => pb::SlruKind::MultiXactMember,
        SlruKind::CommitTs => pb::SlruKind::CommitTs,
    }
}

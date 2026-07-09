//! Pure pageserver seam over page-store reconstruction data and postgres-redo.

mod basebackup;
mod connect_rpc;
mod error;
mod live_ingest;
mod redo;
mod rpc;
mod seed;
mod service;
mod store;

pub use basebackup::{BasebackupPage, BasebackupPayloadInput, encode_basebackup_payload};
pub use error::{PageServiceError, RedoDecodeError, RedoReconstructionError};
pub use live_ingest::{LiveIngestRecord, ingest_live_records};
pub use redo::{PageRedo, PostgresRedo, RedoRecordDecoder, SyntheticRedoCodec};
pub use rpc::{
    BasebackupMetadataRpcRequest, BasebackupMetadataRpcResponse, BasebackupRelMetaRpcMetadata,
    BasebackupSlruPageRpcMetadata, CreateTimelineRpcRequest, CreateTimelineRpcResponse,
    GetPageRpcRequest, GetPageRpcResponse, PageServerErrorBody, PageServerRpcClient,
    PageServerRpcError, PageServerRpcRouter, PutPageImageRpcRequest, PutPageImageRpcResponse,
    PutWalRecordRpcRequest, PutWalRecordRpcResponse, RpcTimeline,
};
pub use seed::{SeedPageImage, relation_file_seed_pages, seed_page_images};
pub use service::{
    BasebackupMetadataRequest, BasebackupMetadataResponse, BasebackupRelMetaMetadata,
    BasebackupSlruPageMetadata, CreateBranchRequest, CreateBranchResponse, CreateTimelineResponse,
    DeleteTimelineResponse, GetPageRequest, GetPageResponse, ListTimelinesResponse, PageService,
    PutPageImageRequest, PutPageImageResponse, PutWalRecordRequest, PutWalRecordResponse,
    RelMetaRequest, RelMetaResponse, RelationSizeRequest, RelationSizeResponse, SlruPageRequest,
    SlruPageResponse,
};
pub use store::{BranchId, InMemoryTimelineStore, TimelineAncestry, TimelineKey};

/// Build the Connect-RPC [`axum::Router`] for the Pageserver service.
pub fn connect_router<R>(service: std::sync::Arc<PageService<R>>) -> axum::Router
where
    R: PageRedo + Send + Sync + 'static,
{
    connect_rpc::router(service)
}

/// Generated protobuf + Connect server stubs.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.pageserver.v1.rs"));
}

//! Blocking client for the `PostgreSQL` compute/pageserver boundary.

use std::{
    cell::RefCell,
    fmt::{self, Display, Formatter},
    time::Duration,
};

use prost::Message;
use reqwest::{Url, blocking::Client as HttpClient};
use thiserror::Error;

/// Stable C ABI layout and exported functions for the future `PostgreSQL` extension boundary.
pub mod ffi;

/// Generated protobuf messages shared with `crabka-pageserver`.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.pageserver.v1.rs"));
}

use ffi::{
    CRABKA_COMPUTE_FFI_VERSION, CRABKA_COMPUTE_FORK_FREE_SPACE_MAP, CRABKA_COMPUTE_FORK_INIT,
    CRABKA_COMPUTE_FORK_MAIN, CRABKA_COMPUTE_FORK_VISIBILITY_MAP, CrabkaComputeBasebackupRequest,
    CrabkaComputeBorrowedBytes, CrabkaComputePageFetchRequest, CrabkaComputeTimelineSeedRequest,
};

/// A validated, non-empty identifier used by pageserver routing paths.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageServerIdentifier(String);

impl PageServerIdentifier {
    /// Parses a pageserver identifier from an owned string.
    ///
    /// # Errors
    ///
    /// Returns [`ComputeClientError::EmptyIdentifier`] when `value` is empty or
    /// contains only whitespace.
    pub fn parse(value: impl Into<String>) -> Result<Self, ComputeClientError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ComputeClientError::EmptyIdentifier);
        }

        Ok(Self(value))
    }

    /// Returns the identifier as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for PageServerIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for PageServerIdentifier {
    type Error = ComputeClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for PageServerIdentifier {
    type Error = ComputeClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

/// Pageserver tenant identifier.
pub type TenantId = PageServerIdentifier;

/// Pageserver timeline identifier.
pub type TimelineId = PageServerIdentifier;

/// `PostgreSQL` LSN carried as an integer until the wire representation is chosen.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Lsn(u64);

impl Lsn {
    /// Creates an LSN from its raw integer value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw integer value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for Lsn {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value())
    }
}

impl From<Lsn> for u64 {
    fn from(value: Lsn) -> Self {
        value.value()
    }
}

/// `PostgreSQL` relation file node named in a page fetch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelFileNode(u32);

impl RelFileNode {
    /// Creates a relation file node from its raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl Display for RelFileNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value())
    }
}

impl From<RelFileNode> for u32 {
    fn from(value: RelFileNode) -> Self {
        value.value()
    }
}

/// `PostgreSQL` tablespace OID named in a page fetch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TablespaceOid(u32);

impl TablespaceOid {
    /// Creates a tablespace OID from its raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl Display for TablespaceOid {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value())
    }
}

impl From<TablespaceOid> for u32 {
    fn from(value: TablespaceOid) -> Self {
        value.value()
    }
}

/// `PostgreSQL` database OID named in a page fetch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseOid(u32);

impl DatabaseOid {
    /// Creates a database OID from its raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw `PostgreSQL` identifier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl Display for DatabaseOid {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value())
    }
}

impl From<DatabaseOid> for u32 {
    fn from(value: DatabaseOid) -> Self {
        value.value()
    }
}

/// `PostgreSQL` block number named in a page fetch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlockNumber(u32);

impl BlockNumber {
    /// Creates a block number from its raw `PostgreSQL` value.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw `PostgreSQL` value.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl Display for BlockNumber {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value())
    }
}

impl From<BlockNumber> for u32 {
    fn from(value: BlockNumber) -> Self {
        value.value()
    }
}

/// `PostgreSQL` relation fork requested from the pageserver.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForkName {
    /// Main relation fork.
    Main,
    /// Free-space-map fork.
    FreeSpaceMap,
    /// Visibility-map fork.
    VisibilityMap,
    /// Initialization fork for unlogged relations.
    Init,
}

impl ForkName {
    /// Returns the path segment used by the JSON request shape.
    #[must_use]
    pub const fn as_path_segment(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::FreeSpaceMap => "fsm",
            Self::VisibilityMap => "vm",
            Self::Init => "init",
        }
    }

    /// Returns the stable FFI discriminator used by the header stub.
    #[must_use]
    pub const fn ffi_discriminator(self) -> u32 {
        match self {
            Self::Main => CRABKA_COMPUTE_FORK_MAIN,
            Self::FreeSpaceMap => CRABKA_COMPUTE_FORK_FREE_SPACE_MAP,
            Self::VisibilityMap => CRABKA_COMPUTE_FORK_VISIBILITY_MAP,
            Self::Init => CRABKA_COMPUTE_FORK_INIT,
        }
    }
}

impl Display for ForkName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_path_segment())
    }
}

/// Request to seed a new timeline from an ancestor timeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelineSeedRequest {
    /// Tenant that owns both timelines.
    pub tenant_id: TenantId,
    /// Timeline being created for the compute node.
    pub timeline_id: TimelineId,
    /// Ancestor timeline used as the copy source.
    pub ancestor_timeline_id: TimelineId,
    /// LSN at which the new timeline branches.
    pub ancestor_start_lsn: Lsn,
}

impl TimelineSeedRequest {
    /// Returns the transport-neutral JSON request shape.
    #[must_use]
    pub fn to_shape(&self) -> PageServerRequestShape {
        PageServerRequestShape::post(format!(
            "/v1/tenants/{}/timelines/{}/seed",
            self.tenant_id, self.timeline_id
        ))
        .with_query(
            "ancestor_timeline_id",
            self.ancestor_timeline_id.to_string(),
        )
        .with_query("ancestor_start_lsn", self.ancestor_start_lsn.to_string())
    }

    /// Returns the borrowed C ABI layout for this request.
    #[must_use]
    pub fn as_ffi(&self) -> CrabkaComputeTimelineSeedRequest {
        CrabkaComputeTimelineSeedRequest {
            version: CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes(self.tenant_id.as_str()),
            timeline_id: borrowed_bytes(self.timeline_id.as_str()),
            ancestor_timeline_id: borrowed_bytes(self.ancestor_timeline_id.as_str()),
            ancestor_start_lsn: self.ancestor_start_lsn.into(),
        }
    }
}

/// Request to start a basebackup stream for a compute node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BasebackupRequest {
    /// Tenant that owns the requested timeline.
    pub tenant_id: TenantId,
    /// Timeline to export.
    pub timeline_id: TimelineId,
    /// Consistent LSN for the basebackup.
    pub lsn: Lsn,
}

impl BasebackupRequest {
    /// Returns the transport-neutral JSON request shape.
    #[must_use]
    pub fn to_shape(&self) -> PageServerRequestShape {
        PageServerRequestShape::get(format!(
            "/v1/tenants/{}/timelines/{}/basebackup",
            self.tenant_id, self.timeline_id
        ))
        .with_query("lsn", self.lsn.to_string())
    }

    /// Returns the borrowed C ABI layout for this request.
    #[must_use]
    pub fn as_ffi(&self) -> CrabkaComputeBasebackupRequest {
        CrabkaComputeBasebackupRequest {
            version: CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes(self.tenant_id.as_str()),
            timeline_id: borrowed_bytes(self.timeline_id.as_str()),
            lsn: self.lsn.into(),
        }
    }
}

/// Request to fetch one `PostgreSQL` page for the compute `smgr` hook.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageFetchRequest {
    /// Tenant that owns the requested timeline.
    pub tenant_id: TenantId,
    /// Timeline to read.
    pub timeline_id: TimelineId,
    /// Tablespace containing the relation.
    pub tablespace_oid: TablespaceOid,
    /// Database containing the relation.
    pub database_oid: DatabaseOid,
    /// Relation file node containing the page.
    pub relfilenode: RelFileNode,
    /// Relation fork containing the page.
    pub fork_name: ForkName,
    /// Block number within the fork.
    pub block_number: BlockNumber,
    /// Page image LSN requested by compute.
    pub request_lsn: Lsn,
}

impl PageFetchRequest {
    /// Returns the transport-neutral JSON request shape.
    #[must_use]
    pub fn to_shape(&self) -> PageServerRequestShape {
        PageServerRequestShape::get(format!(
            "/v1/tenants/{}/timelines/{}/pages/{}/{}/{}/{}/{}",
            self.tenant_id,
            self.timeline_id,
            self.tablespace_oid,
            self.database_oid,
            self.relfilenode,
            self.fork_name,
            self.block_number
        ))
        .with_query("request_lsn", self.request_lsn.to_string())
    }

    /// Returns the borrowed C ABI layout for this request.
    #[must_use]
    pub fn as_ffi(&self) -> CrabkaComputePageFetchRequest {
        CrabkaComputePageFetchRequest {
            version: CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes(self.tenant_id.as_str()),
            timeline_id: borrowed_bytes(self.timeline_id.as_str()),
            tablespace_oid: self.tablespace_oid.into(),
            database_oid: self.database_oid.into(),
            relfilenode: self.relfilenode.into(),
            fork_name: self.fork_name.ffi_discriminator(),
            block_number: self.block_number.into(),
            request_lsn: self.request_lsn.into(),
        }
    }
}

fn borrowed_bytes(value: &str) -> CrabkaComputeBorrowedBytes {
    CrabkaComputeBorrowedBytes {
        ptr: value.as_ptr().cast(),
        len: value.len(),
    }
}

/// Page-server operation represented by the compute client boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PageServerOperation {
    /// Seed a timeline from an ancestor.
    SeedTimeline,
    /// Start a basebackup.
    StartBasebackup,
    /// Fetch one `PostgreSQL` page.
    FetchPage,
}

impl Display for PageServerOperation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let operation = match self {
            Self::SeedTimeline => "seed timeline",
            Self::StartBasebackup => "start basebackup",
            Self::FetchPage => "fetch page",
        };
        formatter.write_str(operation)
    }
}

/// Minimal HTTP-like method used only for request shaping tests.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PageServerMethod {
    /// Read-only request.
    Get,
    /// Mutating request.
    Post,
}

/// Transport-neutral request shape consumed by future HTTP/gRPC adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageServerRequestShape {
    /// Placeholder method selected by the request type.
    pub method: PageServerMethod,
    /// Placeholder path selected by the request type.
    pub path: String,
    /// Placeholder query pairs selected by the request type.
    pub query: Vec<(String, String)>,
}

impl PageServerRequestShape {
    fn get(path: String) -> Self {
        Self::new(PageServerMethod::Get, path)
    }

    fn post(path: String) -> Self {
        Self::new(PageServerMethod::Post, path)
    }

    fn new(method: PageServerMethod, path: String) -> Self {
        Self {
            method,
            path,
            query: Vec::new(),
        }
    }

    fn with_query(mut self, key: &str, value: String) -> Self {
        self.query.push((key.to_owned(), value));
        self
    }
}

/// Compute-side request enum used by local request-shape tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComputeClientRequest {
    /// Seed a timeline before compute starts replay.
    SeedTimeline(TimelineSeedRequest),
    /// Start a basebackup for compute bootstrap.
    StartBasebackup(BasebackupRequest),
    /// Fetch one page from the pageserver.
    FetchPage(PageFetchRequest),
}

impl ComputeClientRequest {
    /// Returns the operation represented by this request.
    #[must_use]
    pub const fn operation(&self) -> PageServerOperation {
        match self {
            Self::SeedTimeline(_) => PageServerOperation::SeedTimeline,
            Self::StartBasebackup(_) => PageServerOperation::StartBasebackup,
            Self::FetchPage(_) => PageServerOperation::FetchPage,
        }
    }

    /// Returns the transport-neutral request shape.
    #[must_use]
    pub fn to_shape(&self) -> PageServerRequestShape {
        match self {
            Self::SeedTimeline(request) => request.to_shape(),
            Self::StartBasebackup(request) => request.to_shape(),
            Self::FetchPage(request) => request.to_shape(),
        }
    }
}

const DEFAULT_BRANCH_ID: &str = "main";
const CONNECT_PROTOCOL_VERSION: &str = "1";
const CONNECT_PROTO_CONTENT_TYPE: &str = "application/proto";
const GET_PAGE_PATH: &str = "/crabka.pageserver.v1.PageService/GetPage";
const BASEBACKUP_PATH: &str = "/crabka.pageserver.v1.PageService/Basebackup";
const CREATE_BRANCH_PATH: &str = "/crabka.pageserver.v1.PageService/CreateBranch";
const SEED_IMAGE_PATH: &str = "/crabka.pageserver.v1.PageService/SeedImage";
const INGEST_WAL_PATH: &str = "/crabka.pageserver.v1.PageService/IngestWal";

/// Request to seed one exact page image through the pageserver transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageSeedImageRequest {
    /// Tenant that owns the requested timeline.
    pub tenant_id: TenantId,
    /// Timeline to mutate.
    pub timeline_id: TimelineId,
    /// Tablespace containing the relation.
    pub tablespace_oid: TablespaceOid,
    /// Database containing the relation.
    pub database_oid: DatabaseOid,
    /// Relation file node containing the page.
    pub relfilenode: RelFileNode,
    /// Relation fork containing the page.
    pub fork_name: ForkName,
    /// Block number within the fork.
    pub block_number: BlockNumber,
    /// LSN at which the image becomes visible.
    pub lsn: Lsn,
    /// Exact `PostgreSQL` page image.
    pub page: Vec<u8>,
}

/// Request to ingest one live WAL delta through the pageserver transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveWalIngestRequest {
    /// Tenant that owns the requested timeline.
    pub tenant_id: TenantId,
    /// Timeline to mutate.
    pub timeline_id: TimelineId,
    /// Tablespace containing the relation.
    pub tablespace_oid: TablespaceOid,
    /// Database containing the relation.
    pub database_oid: DatabaseOid,
    /// Relation file node containing the page.
    pub relfilenode: RelFileNode,
    /// Relation fork containing the page.
    pub fork_name: ForkName,
    /// Block number within the fork.
    pub block_number: BlockNumber,
    /// LSN at which the WAL record becomes visible.
    pub lsn: Lsn,
    /// Whether the WAL record initializes the page.
    pub will_init: bool,
    /// Opaque WAL record bytes understood by the pageserver redo codec.
    pub record: Vec<u8>,
}

/// Blocking Connect-compatible pageserver transport for compute `smgr` calls.
#[derive(Clone, Debug)]
pub struct BlockingPageServerClient {
    endpoint: Url,
    http: HttpClient,
}

impl BlockingPageServerClient {
    /// Creates a client for a pageserver Connect-RPC endpoint.
    pub fn connect(endpoint: &str) -> Result<Self, ComputeClientError> {
        let endpoint = parse_endpoint(endpoint)?;
        let http = HttpClient::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|source| ComputeClientError::Transport {
                message: source.to_string(),
            })?;

        Ok(Self { endpoint, http })
    }

    fn post_protobuf<Request, Response>(
        &self,
        path: &str,
        request: &Request,
    ) -> Result<Response, ComputeClientError>
    where
        Request: Message,
        Response: Message + Default,
    {
        let url = self
            .endpoint
            .join(path)
            .map_err(|source| ComputeClientError::Transport {
                message: source.to_string(),
            })?;
        let response = self
            .http
            .post(url)
            .header("content-type", CONNECT_PROTO_CONTENT_TYPE)
            .header("connect-protocol-version", CONNECT_PROTOCOL_VERSION)
            .body(request.encode_to_vec())
            .send()
            .map_err(|source| ComputeClientError::Transport {
                message: source.to_string(),
            })?;
        let status = response.status();
        let body = response
            .bytes()
            .map_err(|source| ComputeClientError::Transport {
                message: source.to_string(),
            })?;

        if !status.is_success() {
            return Err(ComputeClientError::RemoteStatus {
                status: status.as_u16(),
                message: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        Response::decode(body).map_err(|source| ComputeClientError::Decode {
            message: source.to_string(),
        })
    }

    /// Seeds one exact page image through the pageserver Connect-RPC surface.
    pub fn seed_page_image(&self, request: PageSeedImageRequest) -> Result<(), ComputeClientError> {
        let page = PageImage::from_bytes(request.page)?;
        let seed_image = pb::SeedImageRequest {
            timeline: Some(pb_timeline(&request.tenant_id, &request.timeline_id)),
            key: Some(pb_page_key(
                request.tablespace_oid,
                request.database_oid,
                request.relfilenode,
                request.fork_name,
                request.block_number,
            )),
            lsn: request.lsn.value(),
            page: page.bytes,
        };
        let _: pb::SeedImageResponse = self.post_protobuf(SEED_IMAGE_PATH, &seed_image)?;
        Ok(())
    }

    /// Ingests one live WAL record through the pageserver Connect-RPC surface.
    pub fn ingest_wal(&self, request: LiveWalIngestRequest) -> Result<(), ComputeClientError> {
        let ingest_wal = pb::IngestWalRequest {
            timeline: Some(pb_timeline(&request.tenant_id, &request.timeline_id)),
            key: Some(pb_page_key(
                request.tablespace_oid,
                request.database_oid,
                request.relfilenode,
                request.fork_name,
                request.block_number,
            )),
            lsn: request.lsn.value(),
            will_init: request.will_init,
            record: request.record,
        };
        let _: pb::IngestWalResponse = self.post_protobuf(INGEST_WAL_PATH, &ingest_wal)?;
        Ok(())
    }
}

impl ComputePageServerClient for BlockingPageServerClient {
    fn seed_timeline(&self, request: TimelineSeedRequest) -> Result<(), ComputeClientError> {
        let create_branch = pb::CreateBranchRequest {
            timeline: Some(pb_timeline(&request.tenant_id, &request.timeline_id)),
            source_timeline: Some(pb_timeline(
                &request.tenant_id,
                &request.ancestor_timeline_id,
            )),
            branch_lsn: request.ancestor_start_lsn.value(),
        };
        let _: pb::CreateBranchResponse = self.post_protobuf(CREATE_BRANCH_PATH, &create_branch)?;
        Ok(())
    }

    fn start_basebackup(
        &self,
        request: BasebackupRequest,
    ) -> Result<BasebackupStream, ComputeClientError> {
        let basebackup = pb::BasebackupRequest {
            timeline: Some(pb_timeline(&request.tenant_id, &request.timeline_id)),
            lsn: request.lsn.value(),
        };
        let response: pb::BasebackupResponse = self.post_protobuf(BASEBACKUP_PATH, &basebackup)?;
        BasebackupStream::from_response(response)
    }

    fn fetch_page(&self, request: PageFetchRequest) -> Result<PageImage, ComputeClientError> {
        let get_page = pb::GetPageRequest {
            timeline: Some(pb_timeline(&request.tenant_id, &request.timeline_id)),
            key: Some(pb::PageKey {
                rel: Some(pb_rel_tag(&request)),
                block_number: request.block_number.value(),
            }),
            lsn: request.request_lsn.value(),
        };
        let response: pb::GetPageResponse = self.post_protobuf(GET_PAGE_PATH, &get_page)?;
        PageImage::from_bytes(response.page)
    }
}

fn parse_endpoint(endpoint: &str) -> Result<Url, ComputeClientError> {
    if endpoint.trim().is_empty() {
        return Err(ComputeClientError::EmptyEndpoint);
    }

    let mut url = Url::parse(endpoint).map_err(|source| ComputeClientError::InvalidEndpoint {
        message: source.to_string(),
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ComputeClientError::InvalidEndpoint {
            message: "pageserver endpoint must use http or https".to_owned(),
        });
    }
    if url.path().is_empty() {
        url.set_path("/");
    }
    Ok(url)
}

fn pb_timeline(tenant_id: &TenantId, timeline_id: &TimelineId) -> pb::Timeline {
    pb::Timeline {
        branch_id: DEFAULT_BRANCH_ID.to_owned(),
        tenant_id: tenant_id.to_string(),
        timeline_id: timeline_id.to_string(),
    }
}

fn pb_rel_tag(request: &PageFetchRequest) -> pb::RelTag {
    pb::RelTag {
        spc_node: request.tablespace_oid.value(),
        db_node: request.database_oid.value(),
        rel_node: request.relfilenode.value(),
        fork_number: request.fork_name.ffi_discriminator(),
    }
}

fn pb_page_key(
    tablespace_oid: TablespaceOid,
    database_oid: DatabaseOid,
    relfilenode: RelFileNode,
    fork_name: ForkName,
    block_number: BlockNumber,
) -> pb::PageKey {
    pb::PageKey {
        rel: Some(pb::RelTag {
            spc_node: tablespace_oid.value(),
            db_node: database_oid.value(),
            rel_node: relfilenode.value(),
            fork_number: fork_name.ffi_discriminator(),
        }),
        block_number: block_number.value(),
    }
}

/// Page data returned by the pageserver transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageImage {
    /// Raw `PostgreSQL` page bytes.
    pub bytes: Vec<u8>,
}

impl PageImage {
    /// Parses an exact `PostgreSQL` page image.
    pub fn parse(bytes: Vec<u8>) -> Result<Self, ComputeClientError> {
        Self::from_bytes(bytes)
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, ComputeClientError> {
        if bytes.len() != ffi::CRABKA_COMPUTE_PAGE_SIZE {
            return Err(ComputeClientError::InvalidPageImageSize { found: bytes.len() });
        }

        Ok(Self { bytes })
    }
}

/// Basebackup descriptor returned by the pageserver transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BasebackupStream {
    /// Opaque stream identifier derived from the returned timeline and payload.
    pub stream_id: PageServerIdentifier,
    /// Consistent LSN returned by the pageserver.
    pub lsn: Lsn,
    /// Deterministic basebackup payload bytes returned by the pageserver.
    pub tar: Vec<u8>,
    /// Number of relation metadata records included in the payload.
    pub relmeta_count: usize,
    /// Number of SLRU pages included in the payload.
    pub slru_page_count: usize,
}

impl BasebackupStream {
    fn from_response(response: pb::BasebackupResponse) -> Result<Self, ComputeClientError> {
        let timeline = response
            .timeline
            .as_ref()
            .ok_or(ComputeClientError::MissingResponseField { field: "timeline" })?;
        let stream_id = PageServerIdentifier::parse(format!(
            "basebackup:{}:{}:{}",
            timeline.tenant_id,
            timeline.timeline_id,
            response.tar.len()
        ))?;

        Ok(Self {
            stream_id,
            lsn: Lsn::new(response.lsn),
            relmeta_count: response.relmeta.len(),
            slru_page_count: response.slru_pages.len(),
            tar: response.tar,
        })
    }
}

/// Errors raised by the compute-client transport.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ComputeClientError {
    /// Identifier parsing rejected an empty value.
    #[error("pageserver identifiers must not be empty")]
    EmptyIdentifier,
    /// Endpoint parsing rejected an empty value.
    #[error("pageserver endpoint must not be empty")]
    EmptyEndpoint,
    /// Endpoint parsing rejected the supplied URL.
    #[error("invalid pageserver endpoint: {message}")]
    InvalidEndpoint {
        /// Endpoint parse failure.
        message: String,
    },
    /// HTTP transport failed before a response was decoded.
    #[error("pageserver transport failed: {message}")]
    Transport {
        /// Transport failure.
        message: String,
    },
    /// Pageserver returned a non-success status.
    #[error("pageserver returned HTTP {status}: {message}")]
    RemoteStatus {
        /// HTTP status code.
        status: u16,
        /// Response body or diagnostic text.
        message: String,
    },
    /// Protobuf response decoding failed.
    #[error("pageserver response decode failed: {message}")]
    Decode {
        /// Decode failure.
        message: String,
    },
    /// Protobuf response omitted a required semantic field.
    #[error("pageserver response is missing `{field}`")]
    MissingResponseField {
        /// Missing field name.
        field: &'static str,
    },
    /// `GetPage` returned bytes that are not exactly one `PostgreSQL` page.
    #[error("pageserver returned a {found}-byte page image; expected 8192 bytes")]
    InvalidPageImageSize {
        /// Returned byte count.
        found: usize,
    },
}

/// Compute client boundary that future transport adapters will implement.
pub trait ComputePageServerClient {
    /// Seeds a timeline for compute bootstrap.
    fn seed_timeline(&self, request: TimelineSeedRequest) -> Result<(), ComputeClientError>;

    /// Starts a basebackup stream for compute bootstrap.
    fn start_basebackup(
        &self,
        request: BasebackupRequest,
    ) -> Result<BasebackupStream, ComputeClientError>;

    /// Fetches one page for the future `PostgreSQL` `smgr` hook.
    fn fetch_page(&self, request: PageFetchRequest) -> Result<PageImage, ComputeClientError>;
}

/// Deterministic in-process transport for request-shaping tests and examples.
#[derive(Debug, Default)]
pub struct LocalMockComputePageServerClient {
    recorded_requests: RefCell<Vec<RecordedMockRequest>>,
}

impl LocalMockComputePageServerClient {
    /// Creates an empty local mock transport.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the shaped requests recorded so far.
    #[must_use]
    pub fn recorded_requests(&self) -> Vec<RecordedMockRequest> {
        self.recorded_requests.borrow().clone()
    }

    fn record(&self, request: &ComputeClientRequest) {
        self.recorded_requests
            .borrow_mut()
            .push(RecordedMockRequest::from_request(request));
    }
}

impl ComputePageServerClient for LocalMockComputePageServerClient {
    fn seed_timeline(&self, request: TimelineSeedRequest) -> Result<(), ComputeClientError> {
        self.record(&ComputeClientRequest::SeedTimeline(request));
        Ok(())
    }

    fn start_basebackup(
        &self,
        request: BasebackupRequest,
    ) -> Result<BasebackupStream, ComputeClientError> {
        self.record(&ComputeClientRequest::StartBasebackup(request.clone()));
        Ok(BasebackupStream {
            stream_id: PageServerIdentifier::parse("local-mock-basebackup")?,
            lsn: request.lsn,
            tar: Vec::new(),
            relmeta_count: 0,
            slru_page_count: 0,
        })
    }

    fn fetch_page(&self, request: PageFetchRequest) -> Result<PageImage, ComputeClientError> {
        let block_number = request.block_number;
        self.record(&ComputeClientRequest::FetchPage(request));
        Ok(PageImage {
            bytes: deterministic_page_image(block_number),
        })
    }
}

/// A shaped request captured by [`LocalMockComputePageServerClient`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedMockRequest {
    /// Operation requested by the compute-side caller.
    pub operation: PageServerOperation,
    /// Transport-neutral request shape that a real adapter would serialize.
    pub shape: PageServerRequestShape,
}

impl RecordedMockRequest {
    fn from_request(request: &ComputeClientRequest) -> Self {
        Self {
            operation: request.operation(),
            shape: request.to_shape(),
        }
    }
}

fn deterministic_page_image(block_number: BlockNumber) -> Vec<u8> {
    const PAGE_SIZE: usize = 8_192;
    let mut bytes = vec![0; PAGE_SIZE];
    let block_bytes = block_number.value().to_le_bytes();
    bytes[..block_bytes.len()].copy_from_slice(&block_bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn timeline_seed_request_shapes_path_and_query() -> Result<(), ComputeClientError> {
        let request = TimelineSeedRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            ancestor_timeline_id: TimelineId::try_from("timeline-root")?,
            ancestor_start_lsn: Lsn::new(128),
        };

        let shape = request.to_shape();

        assert!(
            shape
                == PageServerRequestShape {
                    method: PageServerMethod::Post,
                    path: "/v1/tenants/tenant-a/timelines/timeline-b/seed".to_owned(),
                    query: vec![
                        (
                            "ancestor_timeline_id".to_owned(),
                            "timeline-root".to_owned()
                        ),
                        ("ancestor_start_lsn".to_owned(), "128".to_owned()),
                    ],
                }
        );
        Ok(())
    }

    #[test]
    fn basebackup_request_shapes_path_and_lsn() -> Result<(), ComputeClientError> {
        let request = BasebackupRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            lsn: Lsn::new(256),
        };

        let shape = request.to_shape();

        assert!(
            shape
                == PageServerRequestShape {
                    method: PageServerMethod::Get,
                    path: "/v1/tenants/tenant-a/timelines/timeline-b/basebackup".to_owned(),
                    query: vec![("lsn".to_owned(), "256".to_owned())],
                }
        );
        Ok(())
    }

    #[test]
    fn page_fetch_request_shapes_relation_and_lsn() -> Result<(), ComputeClientError> {
        let request = PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::VisibilityMap,
            block_number: BlockNumber::new(7),
            request_lsn: Lsn::new(512),
        };

        let shape = ComputeClientRequest::FetchPage(request).to_shape();

        assert!(
            shape
                == PageServerRequestShape {
                    method: PageServerMethod::Get,
                    path: "/v1/tenants/tenant-a/timelines/timeline-b/pages/1663/5/42/vm/7"
                        .to_owned(),
                    query: vec![("request_lsn".to_owned(), "512".to_owned())],
                }
        );
        Ok(())
    }

    #[test]
    fn local_mock_records_all_request_shapes_in_order() -> Result<(), ComputeClientError> {
        let client = LocalMockComputePageServerClient::new();
        let seed_request = TimelineSeedRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            ancestor_timeline_id: TimelineId::try_from("timeline-root")?,
            ancestor_start_lsn: Lsn::new(128),
        };
        let basebackup_request = BasebackupRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            lsn: Lsn::new(256),
        };
        let page_request = PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::Main,
            block_number: BlockNumber::new(7),
            request_lsn: Lsn::new(512),
        };

        client.seed_timeline(seed_request.clone())?;
        let basebackup = client.start_basebackup(basebackup_request.clone())?;
        let page = client.fetch_page(page_request.clone())?;

        assert!(basebackup.stream_id.as_str() == "local-mock-basebackup");
        assert!(page.bytes.len() == 8_192);
        assert!(&page.bytes[..4] == 7_u32.to_le_bytes());
        assert!(
            client.recorded_requests()
                == vec![
                    RecordedMockRequest {
                        operation: PageServerOperation::SeedTimeline,
                        shape: seed_request.to_shape(),
                    },
                    RecordedMockRequest {
                        operation: PageServerOperation::StartBasebackup,
                        shape: basebackup_request.to_shape(),
                    },
                    RecordedMockRequest {
                        operation: PageServerOperation::FetchPage,
                        shape: page_request.to_shape(),
                    },
                ]
        );
        Ok(())
    }

    #[test]
    fn ffi_layout_carries_request_fields_without_allocation() -> Result<(), ComputeClientError> {
        let request = PageFetchRequest {
            tenant_id: TenantId::try_from("tenant-a")?,
            timeline_id: TimelineId::try_from("timeline-b")?,
            tablespace_oid: TablespaceOid::new(1663),
            database_oid: DatabaseOid::new(5),
            relfilenode: RelFileNode::new(42),
            fork_name: ForkName::VisibilityMap,
            block_number: BlockNumber::new(7),
            request_lsn: Lsn::new(512),
        };

        let ffi_request = request.as_ffi();

        assert!(ffi_request.version == ffi::CRABKA_COMPUTE_FFI_VERSION);
        assert!(ffi_request.tenant_id.ptr == request.tenant_id.as_str().as_ptr().cast());
        assert!(ffi_request.tenant_id.len == "tenant-a".len());
        assert!(ffi_request.timeline_id.ptr == request.timeline_id.as_str().as_ptr().cast());
        assert!(ffi_request.timeline_id.len == "timeline-b".len());
        assert!(ffi_request.tablespace_oid == 1663);
        assert!(ffi_request.database_oid == 5);
        assert!(ffi_request.relfilenode == 42);
        assert!(ffi_request.fork_name == ffi::CRABKA_COMPUTE_FORK_VISIBILITY_MAP);
        assert!(ffi_request.block_number == 7);
        assert!(ffi_request.request_lsn == 512);
        Ok(())
    }

    #[test]
    fn ffi_layout_sizes_are_pointer_width_stable() {
        use std::mem::{align_of, size_of};

        assert!(
            size_of::<ffi::CrabkaComputeBorrowedBytes>()
                == size_of::<*const i8>() + size_of::<usize>()
        );
        assert!(align_of::<ffi::CrabkaComputeBorrowedBytes>() == align_of::<usize>());
        assert!(size_of::<ffi::CrabkaComputePageFetchRequest>() >= 64);
        assert!(align_of::<ffi::CrabkaComputePageFetchRequest>() == align_of::<usize>());
    }

    #[test]
    fn identifier_parser_rejects_empty_values() {
        assert!(TenantId::try_from("") == Err(ComputeClientError::EmptyIdentifier));
        assert!(TenantId::try_from("   ") == Err(ComputeClientError::EmptyIdentifier));
    }
}

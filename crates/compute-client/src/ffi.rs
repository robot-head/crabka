//! C ABI boundary for the PostgreSQL compute extension.
//!
//! This module is the crate's only sanctioned unsafe boundary. It owns the
//! `repr(C)` layouts mirrored by `compute/include/crabka_compute_client.h`, the
//! result-code contract, and the exported symbols that C callers compile
//! against while real pageserver transport is still out of scope.

use std::{
    cell::RefCell,
    ffi::{CStr, CString, c_char, c_void},
    ptr, slice,
};

use crate::{
    BlockNumber, BlockingPageServerClient, ComputeClientError, ComputePageServerClient,
    DatabaseOid, ForkName, Lsn, PageFetchRequest, RelFileNode, TablespaceOid, TenantId,
};

/// Current FFI layout revision for requests and responses.
pub const CRABKA_COMPUTE_FFI_VERSION: u32 = 1;

/// Unknown or unset operation discriminator.
pub const CRABKA_COMPUTE_OPERATION_UNKNOWN: u32 = 0;
/// Timeline seed operation discriminator.
pub const CRABKA_COMPUTE_OPERATION_SEED_TIMELINE: u32 = 1;
/// Basebackup operation discriminator.
pub const CRABKA_COMPUTE_OPERATION_START_BASEBACKUP: u32 = 2;
/// Page fetch operation discriminator.
pub const CRABKA_COMPUTE_OPERATION_FETCH_PAGE: u32 = 3;

/// Main relation fork discriminator.
pub const CRABKA_COMPUTE_FORK_MAIN: u32 = 0;
/// Free-space-map relation fork discriminator.
pub const CRABKA_COMPUTE_FORK_FREE_SPACE_MAP: u32 = 1;
/// Visibility-map relation fork discriminator.
pub const CRABKA_COMPUTE_FORK_VISIBILITY_MAP: u32 = 2;
/// Initialization relation fork discriminator.
pub const CRABKA_COMPUTE_FORK_INIT: u32 = 3;

/// Successful response status discriminator.
pub const CRABKA_COMPUTE_STATUS_OK: u32 = 0;
/// Invalid caller input response status discriminator.
pub const CRABKA_COMPUTE_STATUS_INVALID_ARGUMENT: u32 = 1;

/// FFI call completed successfully.
pub const CRABKA_COMPUTE_RESULT_OK: i32 = 0;
/// FFI caller passed an invalid argument.
pub const CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT: i32 = -1;
/// FFI call failed because of an internal invariant violation.
pub const CRABKA_COMPUTE_RESULT_INTERNAL_ERROR: i32 = -2;

/// Raw `PostgreSQL` page size used by the page-fetch FFI.
pub const CRABKA_COMPUTE_PAGE_SIZE: usize = 8_192;

thread_local! {
    static LAST_ERROR_MESSAGE: RefCell<CString> = RefCell::new(empty_c_string());
}

/// Borrowed byte string passed across the `PostgreSQL` extension boundary.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrabkaComputeBorrowedBytes {
    /// Pointer to the first byte, or null when `len` is zero.
    pub ptr: *const c_char,
    /// Number of bytes available at `ptr`.
    pub len: usize,
}

impl CrabkaComputeBorrowedBytes {
    /// Returns an empty borrowed byte string.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            ptr: ptr::null(),
            len: 0,
        }
    }
}

/// Timeline seed request layout expected by the future C shim.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrabkaComputeTimelineSeedRequest {
    /// FFI layout revision.
    pub version: u32,
    /// Tenant identifier bytes.
    pub tenant_id: CrabkaComputeBorrowedBytes,
    /// Timeline identifier bytes.
    pub timeline_id: CrabkaComputeBorrowedBytes,
    /// Ancestor timeline identifier bytes.
    pub ancestor_timeline_id: CrabkaComputeBorrowedBytes,
    /// Ancestor branch LSN.
    pub ancestor_start_lsn: u64,
}

/// Basebackup request layout expected by the future C shim.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrabkaComputeBasebackupRequest {
    /// FFI layout revision.
    pub version: u32,
    /// Tenant identifier bytes.
    pub tenant_id: CrabkaComputeBorrowedBytes,
    /// Timeline identifier bytes.
    pub timeline_id: CrabkaComputeBorrowedBytes,
    /// Consistent backup LSN.
    pub lsn: u64,
}

/// Page fetch request layout expected by the future C shim.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrabkaComputePageFetchRequest {
    /// FFI layout revision.
    pub version: u32,
    /// Tenant identifier bytes.
    pub tenant_id: CrabkaComputeBorrowedBytes,
    /// Timeline identifier bytes.
    pub timeline_id: CrabkaComputeBorrowedBytes,
    /// `PostgreSQL` tablespace OID.
    pub tablespace_oid: u32,
    /// `PostgreSQL` database OID.
    pub database_oid: u32,
    /// `PostgreSQL` relation file node.
    pub relfilenode: u32,
    /// One of the `CRABKA_COMPUTE_FORK_*` constants.
    pub fork_name: u32,
    /// `PostgreSQL` block number.
    pub block_number: u32,
    /// Requested page-image LSN.
    pub request_lsn: u64,
}

/// Response layout returned by future payload-producing FFI calls.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrabkaComputeResponse {
    /// FFI layout revision.
    pub version: u32,
    /// One of the `CRABKA_COMPUTE_STATUS_*` constants.
    pub status: u32,
    /// Optional response payload owned by the future C shim.
    pub payload_ptr: *mut c_void,
    /// Number of bytes available at `payload_ptr` when it is a byte payload.
    pub payload_len: usize,
    /// Optional UTF-8 diagnostic message.
    pub error_message: CrabkaComputeBorrowedBytes,
}

/// Opaque compute-client handle owned by Rust and passed back to C.
pub struct CrabkaComputeClient {
    inner: BlockingPageServerClient,
}

/// Converts an FFI page request into the safe Rust request shape.
///
/// # Safety
///
/// `request` must point to a valid [`CrabkaComputePageFetchRequest`] for the
/// duration of this call. Any non-empty borrowed byte string inside the request
/// must point to valid UTF-8 bytes for its declared length.
pub unsafe fn try_page_fetch_request_from_ffi(
    request: *const CrabkaComputePageFetchRequest,
) -> Result<PageFetchRequest, i32> {
    if request.is_null() {
        set_last_error_message("page fetch request pointer must not be null");
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    }

    // SAFETY: the caller promises `request` points to a valid request for this call.
    let request = unsafe { &*request };
    page_fetch_request_from_ref(request)
}

/// Opens a blocking compute client handle.
///
/// # Safety
///
/// `endpoint` must be a valid NUL-terminated C string when non-null, and
/// `handle_out` must be a valid writable pointer when non-null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ck_connect(
    endpoint: *const c_char,
    handle_out: *mut *mut CrabkaComputeClient,
) -> i32 {
    if endpoint.is_null() {
        set_last_error_message("pageserver endpoint pointer must not be null");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }
    if handle_out.is_null() {
        set_last_error_message("handle output pointer must not be null");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }

    // SAFETY: the caller promises `endpoint` is NUL-terminated and readable.
    let endpoint = unsafe { CStr::from_ptr(endpoint) };
    let Ok(endpoint) = endpoint.to_str() else {
        set_last_error_message("pageserver endpoint must be valid UTF-8");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    };
    if endpoint.is_empty() {
        set_last_error_message("pageserver endpoint must not be empty");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }
    // SAFETY: the caller promises `handle_out` is valid and writable.
    unsafe { handle_out.write(ptr::null_mut()) };

    let client = match BlockingPageServerClient::connect(endpoint) {
        Ok(client) => client,
        Err(err) => return result_code_from_error(&err),
    };
    let handle = Box::into_raw(Box::new(CrabkaComputeClient { inner: client }));

    // SAFETY: the caller promises `handle_out` is valid and writable.
    unsafe { handle_out.write(handle) };
    set_last_error_message("");
    CRABKA_COMPUTE_RESULT_OK
}

/// Fetches one `PostgreSQL` page into `out_page`.
///
/// # Safety
///
/// `request` and `out_page` must be valid for reads/writes of their declared
/// sizes. `handle` must be a handle previously returned by [`ck_connect`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ck_get_page(
    handle: *mut CrabkaComputeClient,
    request: *const CrabkaComputePageFetchRequest,
    out_page: *mut u8,
    out_page_len: usize,
) -> i32 {
    if handle.is_null() {
        set_last_error_message("compute client handle must not be null");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }
    if out_page.is_null() {
        set_last_error_message("page output pointer must not be null");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }
    if out_page_len != CRABKA_COMPUTE_PAGE_SIZE {
        set_last_error_message("page output buffer must be exactly 8192 bytes");
        return CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT;
    }
    // SAFETY: this call performs request pointer and borrowed-byte validation only.
    let request = match unsafe { try_page_fetch_request_from_ffi(request) } {
        Ok(request) => request,
        Err(code) => return code,
    };
    // SAFETY: `handle` is non-null and must have been returned by `ck_connect`.
    let handle = unsafe { &mut *handle };
    let page = match handle.inner.fetch_page(request) {
        Ok(page) => page,
        Err(err) => return result_code_from_error(&err),
    };

    // SAFETY: `out_page` is non-null and `out_page_len` was verified to be one page.
    unsafe { ptr::copy_nonoverlapping(page.bytes.as_ptr(), out_page, page.bytes.len()) };
    set_last_error_message("");
    CRABKA_COMPUTE_RESULT_OK
}

/// Closes a compute client handle returned by [`ck_connect`].
///
/// # Safety
///
/// `handle` must be null or a handle returned by [`ck_connect`] that has not
/// already been passed to `ck_disconnect`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ck_disconnect(handle: *mut CrabkaComputeClient) {
    if handle.is_null() {
        return;
    }

    // SAFETY: the caller transfers ownership of a handle allocated by `ck_connect`.
    unsafe { drop(Box::from_raw(handle)) };
}

/// Returns the current thread's last FFI error message.
#[unsafe(no_mangle)]
pub extern "C" fn ck_last_error_message() -> CrabkaComputeBorrowedBytes {
    LAST_ERROR_MESSAGE.with(|message| {
        let message = message.borrow();
        CrabkaComputeBorrowedBytes {
            ptr: message.as_ptr(),
            len: message.as_bytes().len(),
        }
    })
}

fn page_fetch_request_from_ref(
    request: &CrabkaComputePageFetchRequest,
) -> Result<PageFetchRequest, i32> {
    if request.version != CRABKA_COMPUTE_FFI_VERSION {
        set_last_error_message("page fetch request has an unsupported FFI version");
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    }

    let Some(fork_name) = fork_name_from_ffi(request.fork_name) else {
        set_last_error_message("page fetch request has an unknown fork discriminator");
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    };

    let tenant_id = parse_identifier(request.tenant_id, "tenant identifier")?;
    let timeline_id = parse_identifier(request.timeline_id, "timeline identifier")?;

    Ok(PageFetchRequest {
        tenant_id,
        timeline_id,
        tablespace_oid: TablespaceOid::new(request.tablespace_oid),
        database_oid: DatabaseOid::new(request.database_oid),
        relfilenode: RelFileNode::new(request.relfilenode),
        fork_name,
        block_number: BlockNumber::new(request.block_number),
        request_lsn: Lsn::new(request.request_lsn),
    })
}

fn fork_name_from_ffi(value: u32) -> Option<ForkName> {
    match value {
        CRABKA_COMPUTE_FORK_MAIN => Some(ForkName::Main),
        CRABKA_COMPUTE_FORK_FREE_SPACE_MAP => Some(ForkName::FreeSpaceMap),
        CRABKA_COMPUTE_FORK_VISIBILITY_MAP => Some(ForkName::VisibilityMap),
        CRABKA_COMPUTE_FORK_INIT => Some(ForkName::Init),
        _ => None,
    }
}

fn parse_identifier(
    bytes: CrabkaComputeBorrowedBytes,
    field_name: &'static str,
) -> Result<TenantId, i32> {
    if bytes.ptr.is_null() {
        set_last_error_message(&format_error_message(
            field_name,
            "pointer must not be null",
        ));
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    }

    // SAFETY: caller-side ABI validation requires `ptr` to be readable for `len` bytes.
    let bytes = unsafe { slice::from_raw_parts(bytes.ptr.cast::<u8>(), bytes.len) };
    let Ok(value) = std::str::from_utf8(bytes) else {
        set_last_error_message(&format_error_message(field_name, "must be valid UTF-8"));
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    };
    let Ok(identifier) = TenantId::try_from(value) else {
        set_last_error_message(&format_error_message(field_name, "must not be empty"));
        return Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
    };

    Ok(identifier)
}

fn set_last_error_message(message: &str) {
    LAST_ERROR_MESSAGE.with(|last_error_message| {
        *last_error_message.borrow_mut() = c_string_without_nul(message);
    });
}

fn c_string_without_nul(message: &str) -> CString {
    let bytes_without_nul: Vec<u8> = message
        .as_bytes()
        .iter()
        .copied()
        .filter(|byte| *byte != 0)
        .collect();
    CString::new(bytes_without_nul).unwrap_or_else(|_| empty_c_string())
}

fn empty_c_string() -> CString {
    CString::default()
}

fn format_error_message(field_name: &str, message: &str) -> String {
    format!("{field_name} {message}")
}

fn result_code_from_error(err: &ComputeClientError) -> i32 {
    set_last_error_message(&err.to_string());
    match err {
        ComputeClientError::EmptyEndpoint
        | ComputeClientError::InvalidEndpoint { .. }
        | ComputeClientError::EmptyIdentifier
        | ComputeClientError::InvalidPageImageSize { .. }
        | ComputeClientError::MissingResponseField { .. } => CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT,
        ComputeClientError::Transport { .. }
        | ComputeClientError::RemoteStatus { .. }
        | ComputeClientError::Decode { .. } => CRABKA_COMPUTE_RESULT_INTERNAL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ffi_page_request_parses_into_safe_shape() -> Result<(), i32> {
        let tenant = "tenant-a";
        let timeline = "timeline-b";
        let request = CrabkaComputePageFetchRequest {
            version: CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes(tenant),
            timeline_id: borrowed_bytes(timeline),
            tablespace_oid: 1663,
            database_oid: 5,
            relfilenode: 42,
            fork_name: CRABKA_COMPUTE_FORK_VISIBILITY_MAP,
            block_number: 7,
            request_lsn: 512,
        };

        // SAFETY: `request` and its borrowed strings live for this call.
        let parsed = unsafe { try_page_fetch_request_from_ffi(&raw const request) }?;

        assert!(parsed.tenant_id.as_str() == tenant);
        assert!(parsed.timeline_id.as_str() == timeline);
        assert!(parsed.tablespace_oid == TablespaceOid::new(1663));
        assert!(parsed.database_oid == DatabaseOid::new(5));
        assert!(parsed.relfilenode == RelFileNode::new(42));
        assert!(parsed.fork_name == ForkName::VisibilityMap);
        assert!(parsed.block_number == BlockNumber::new(7));
        assert!(parsed.request_lsn == Lsn::new(512));
        Ok(())
    }

    #[test]
    fn ffi_invalid_request_maps_to_result_code_and_error_message() {
        let request = CrabkaComputePageFetchRequest {
            version: CRABKA_COMPUTE_FFI_VERSION + 1,
            tenant_id: borrowed_bytes("tenant-a"),
            timeline_id: borrowed_bytes("timeline-b"),
            tablespace_oid: 1663,
            database_oid: 5,
            relfilenode: 42,
            fork_name: CRABKA_COMPUTE_FORK_MAIN,
            block_number: 7,
            request_lsn: 512,
        };

        // SAFETY: `request` and its borrowed strings live for this call.
        let result = unsafe { try_page_fetch_request_from_ffi(&raw const request) };

        assert!(result == Err(CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT));
        assert!(
            last_error_message_for_test() == "page fetch request has an unsupported FFI version"
        );
    }

    #[test]
    fn ffi_connect_validates_endpoint_before_transport() {
        let mut handle = ptr::null_mut();

        // SAFETY: a null endpoint intentionally exercises boundary validation.
        let result = unsafe { ck_connect(ptr::null(), &raw mut handle) };

        assert!(result == CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
        assert!(handle.is_null());
        assert!(last_error_message_for_test() == "pageserver endpoint pointer must not be null");
    }

    #[test]
    fn ffi_get_page_rejects_null_handle_before_request_validation() {
        let request = CrabkaComputePageFetchRequest {
            version: CRABKA_COMPUTE_FFI_VERSION,
            tenant_id: borrowed_bytes("tenant-a"),
            timeline_id: borrowed_bytes("timeline-b"),
            tablespace_oid: 1663,
            database_oid: 5,
            relfilenode: 42,
            fork_name: CRABKA_COMPUTE_FORK_MAIN,
            block_number: 7,
            request_lsn: 512,
        };
        let mut page = [0; CRABKA_COMPUTE_PAGE_SIZE];

        // SAFETY: a null handle intentionally exercises boundary validation.
        let result = unsafe {
            ck_get_page(
                ptr::null_mut(),
                &raw const request,
                page.as_mut_ptr(),
                page.len(),
            )
        };

        assert!(result == CRABKA_COMPUTE_RESULT_INVALID_ARGUMENT);
        assert!(last_error_message_for_test() == "compute client handle must not be null");
    }

    #[test]
    fn ffi_request_layout_matches_expected_rust_abi() {
        use std::mem::{align_of, offset_of, size_of};

        assert!(
            size_of::<CrabkaComputeBorrowedBytes>()
                == size_of::<*const c_char>() + size_of::<usize>()
        );
        assert!(align_of::<CrabkaComputeBorrowedBytes>() == align_of::<usize>());
        assert!(offset_of!(CrabkaComputeBorrowedBytes, ptr) == 0);
        assert!(offset_of!(CrabkaComputeBorrowedBytes, len) == size_of::<*const c_char>());

        assert!(offset_of!(CrabkaComputePageFetchRequest, version) == 0);
        assert!(offset_of!(CrabkaComputePageFetchRequest, tenant_id) == align_of::<usize>());
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, timeline_id)
                == align_of::<usize>() + size_of::<CrabkaComputeBorrowedBytes>()
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, tablespace_oid)
                == align_of::<usize>() + (size_of::<CrabkaComputeBorrowedBytes>() * 2)
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, database_oid)
                == offset_of!(CrabkaComputePageFetchRequest, tablespace_oid) + size_of::<u32>()
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, relfilenode)
                == offset_of!(CrabkaComputePageFetchRequest, database_oid) + size_of::<u32>()
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, fork_name)
                == offset_of!(CrabkaComputePageFetchRequest, relfilenode) + size_of::<u32>()
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, block_number)
                == offset_of!(CrabkaComputePageFetchRequest, fork_name) + size_of::<u32>()
        );
        assert!(
            offset_of!(CrabkaComputePageFetchRequest, request_lsn)
                == offset_of!(CrabkaComputePageFetchRequest, block_number)
                    + size_of::<u32>()
                    + size_of::<u32>()
        );
        assert!(align_of::<CrabkaComputePageFetchRequest>() == align_of::<usize>());
    }

    #[test]
    fn checked_in_header_compiles_with_matching_c_abi() {
        let source_path = write_header_probe("c", &c_header_probe_source());

        let output = std::process::Command::new("cc")
            .arg("-std=c11")
            .arg("-fsyntax-only")
            .arg("-I")
            .arg(header_directory())
            .arg(&source_path)
            .output()
            .expect("C compiler should run for compute-client header syntax check");

        assert!(
            output.status.success(),
            "C header probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn checked_in_header_compiles_with_matching_cpp_abi() {
        let source_path = write_header_probe("cpp", &cpp_header_probe_source());

        let output = std::process::Command::new("c++")
            .arg("-std=c++17")
            .arg("-fsyntax-only")
            .arg("-I")
            .arg(header_directory())
            .arg(&source_path)
            .output()
            .expect("C++ compiler should run for compute-client header syntax check");

        assert!(
            output.status.success(),
            "C++ header probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn header_directory() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compute/include")
    }

    fn write_header_probe(extension: &str, source: &str) -> std::path::PathBuf {
        let source_path = std::env::temp_dir().join(format!(
            "crabka-compute-client-header-probe-{}.{}",
            std::process::id(),
            extension
        ));
        std::fs::write(&source_path, source).expect("header probe source should be writable");
        source_path
    }

    fn c_header_probe_source() -> String {
        header_probe_source(StaticAssertSyntax::C)
    }

    fn cpp_header_probe_source() -> String {
        header_probe_source(StaticAssertSyntax::Cpp)
    }

    #[derive(Clone, Copy)]
    enum StaticAssertSyntax {
        C,
        Cpp,
    }

    fn header_probe_source(syntax: StaticAssertSyntax) -> String {
        let mut source =
            String::from("#include <stddef.h>\n#include \"crabka_compute_client.h\"\n");
        push_constant_asserts(&mut source, syntax);
        push_type_asserts(&mut source, syntax);
        push_offset_asserts(&mut source, syntax);
        source.push_str("int main(void) { return 0; }\n");
        source
    }

    fn push_constant_asserts(source: &mut String, syntax: StaticAssertSyntax) {
        for expression in [
            "CRABKA_COMPUTE_FFI_VERSION == 1U",
            "CRABKA_COMPUTE_OPERATION_FETCH_PAGE == 3U",
            "CRABKA_COMPUTE_FORK_VISIBILITY_MAP == 2U",
            "CRABKA_COMPUTE_STATUS_INVALID_ARGUMENT == 1U",
            "CRABKA_COMPUTE_RESULT_INTERNAL_ERROR == -2",
            "CRABKA_COMPUTE_PAGE_SIZE == 8192U",
        ] {
            push_header_assert(source, syntax, expression);
        }
    }

    fn push_type_asserts(source: &mut String, syntax: StaticAssertSyntax) {
        push_type_assert::<CrabkaComputeBorrowedBytes>(
            source,
            syntax,
            "CrabkaComputeBorrowedBytes",
        );
        push_type_assert::<CrabkaComputeTimelineSeedRequest>(
            source,
            syntax,
            "CrabkaComputeTimelineSeedRequest",
        );
        push_type_assert::<CrabkaComputeBasebackupRequest>(
            source,
            syntax,
            "CrabkaComputeBasebackupRequest",
        );
        push_type_assert::<CrabkaComputePageFetchRequest>(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
        );
        push_type_assert::<CrabkaComputeResponse>(source, syntax, "CrabkaComputeResponse");
    }

    fn push_offset_asserts(source: &mut String, syntax: StaticAssertSyntax) {
        use std::mem::offset_of;

        push_offset_assert(
            source,
            syntax,
            "CrabkaComputeBorrowedBytes",
            "ptr",
            offset_of!(CrabkaComputeBorrowedBytes, ptr),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputeBorrowedBytes",
            "len",
            offset_of!(CrabkaComputeBorrowedBytes, len),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "version",
            offset_of!(CrabkaComputePageFetchRequest, version),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "tenant_id",
            offset_of!(CrabkaComputePageFetchRequest, tenant_id),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "timeline_id",
            offset_of!(CrabkaComputePageFetchRequest, timeline_id),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "tablespace_oid",
            offset_of!(CrabkaComputePageFetchRequest, tablespace_oid),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "database_oid",
            offset_of!(CrabkaComputePageFetchRequest, database_oid),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "relfilenode",
            offset_of!(CrabkaComputePageFetchRequest, relfilenode),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "fork_name",
            offset_of!(CrabkaComputePageFetchRequest, fork_name),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "block_number",
            offset_of!(CrabkaComputePageFetchRequest, block_number),
        );
        push_offset_assert(
            source,
            syntax,
            "CrabkaComputePageFetchRequest",
            "request_lsn",
            offset_of!(CrabkaComputePageFetchRequest, request_lsn),
        );
    }

    fn push_type_assert<T>(source: &mut String, syntax: StaticAssertSyntax, c_type: &str) {
        use std::mem::{align_of, size_of};

        push_header_assert(
            source,
            syntax,
            &format!("sizeof({c_type}) == {}", size_of::<T>()),
        );
        match syntax {
            StaticAssertSyntax::C => push_header_assert(
                source,
                syntax,
                &format!("_Alignof({c_type}) == {}", align_of::<T>()),
            ),
            StaticAssertSyntax::Cpp => push_header_assert(
                source,
                syntax,
                &format!("alignof({c_type}) == {}", align_of::<T>()),
            ),
        }
    }

    fn push_offset_assert(
        source: &mut String,
        syntax: StaticAssertSyntax,
        c_type: &str,
        field: &str,
        expected_offset: usize,
    ) {
        push_header_assert(
            source,
            syntax,
            &format!("offsetof({c_type}, {field}) == {expected_offset}"),
        );
    }

    fn push_header_assert(source: &mut String, syntax: StaticAssertSyntax, expression: &str) {
        use std::fmt::Write as _;

        match syntax {
            StaticAssertSyntax::C => {
                writeln!(source, "_Static_assert({expression}, \"{expression}\");")
                    .expect("writing to String should not fail");
            }
            StaticAssertSyntax::Cpp => {
                writeln!(source, "static_assert({expression}, \"{expression}\");")
                    .expect("writing to String should not fail");
            }
        }
    }

    fn borrowed_bytes(value: &str) -> CrabkaComputeBorrowedBytes {
        CrabkaComputeBorrowedBytes {
            ptr: value.as_ptr().cast(),
            len: value.len(),
        }
    }

    fn last_error_message_for_test() -> String {
        let message = ck_last_error_message();
        if message.ptr.is_null() {
            return String::new();
        }
        // SAFETY: `ck_last_error_message` returns a pointer to thread-local bytes
        // valid until the next FFI call on this thread.
        let bytes = unsafe { slice::from_raw_parts(message.ptr.cast::<u8>(), message.len) };
        String::from_utf8(bytes.to_vec()).expect("last error message should be UTF-8")
    }
}

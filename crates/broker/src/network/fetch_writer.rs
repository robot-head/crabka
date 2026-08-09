//! Zero-copy fetch response writer (Increments C + D).
//!
//! The generic dispatch loop writes every response with
//! `Framed<S, LengthDelimitedCodec>::send`, which copies the whole body into
//! the codec's write buffer. `encode_response` already copied that body once
//! to prepend the correlation header. For a 100 KB+ fetch that is hundreds of
//! KB of avoidable `memcpy` per request.
//!
//! This module replaces that path **for Fetch responses only** with an ordered
//! [`WriteOp`] plan:
//!
//! * **Increment C (portable, TLS-safe):** the writer writes the response
//!   header and the envelope metadata inline from userspace, and hands each
//!   partition's records region to the socket as its own segment with a
//!   vectored `write_all`. It does not copy the records bytes through the
//!   codec.
//! * **Increment D (Linux plaintext only):** for large records runs on a
//!   plaintext `TcpStream`, the records region becomes a [`WriteOp::File`]
//!   backed by the segment `.log` fd. The kernel `sendfile(2)` zero-copy path
//!   drains it from the page cache to the NIC and never through userspace. On
//!   TLS, on non-Linux, and for small runs, the writer falls back to the
//!   vectored/`pread` path of Increment C. The wire bytes are identical.
//!
//! ## Framing
//!
//! Kafka frames every response with a 4-byte big-endian length prefix. The
//! length is **not** part of any records or file bytes, so the writer computes
//! it up front from the exact body length (`correlation header + Σ op
//! lengths`) and writes it from userspace before it drains the ops. The writer
//! knows the exact body length without materializing the body: the records and
//! file ops carry their own length, and the inline ops are already-built
//! `Bytes`.

use bytes::{BufMut, Bytes, BytesMut};
use crabka_protocol::{
    Encode, ProtocolError,
    api_key::ApiKey,
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::{
        array::{put_array_len, put_nullable_array_len},
        fixed::{put_i16, put_i32, put_i64},
        string_bytes::{put_compact_string, put_string},
        uuid::put_uuid,
    },
    records::RecordsPayload,
    tagged_fields::{WriteTaggedFields, encode_to_bytes},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    error::BrokerError,
    network::{codec, response_header_len, response_header_v1},
};

/// One ordered segment of the fetch response wire frame.
#[derive(Debug)]
pub enum WriteOp {
    /// Userspace bytes: the length prefix and correlation header, partition
    /// metadata, records length prefixes, and tagged-field trailers. On the
    /// vectored path of Increment C it also holds the resolved records bytes.
    Inline(Bytes),
    /// A records region backed by a segment `.log` file (Increments D + E).
    /// `sendfile(2)` drains it on a plaintext `TcpStream` (Linux + Apple +
    /// FreeBSD/DragonFly). Every other stream uses a buffered `pread` +
    /// `write_all` fallback.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    File(crabka_protocol::records::FileRegion),
}

impl WriteOp {
    /// Byte length this op contributes to the frame body. The frame-length
    /// accounting in tests uses it.
    #[must_use]
    #[cfg(test)]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline(b) => b.len(),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::File(r) => r.len,
        }
    }
}

/// One ordered segment of a `FetchResponse` body write plan.
#[derive(Debug, Clone)]
enum FetchWriteOp {
    Inline(Bytes),
    Records(RecordsPayload),
}

impl FetchWriteOp {
    #[must_use]
    fn len(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::Records(payload) => payload.payload_len(),
        }
    }
}

fn fetch_response_write_plan(
    response: &FetchResponse,
    version: i16,
) -> Result<Vec<FetchWriteOp>, ProtocolError> {
    debug_assert!(
        version >= 4,
        "write plan is only valid for the canonical Fetch codec (v4+)"
    );

    let flex = version >= 12;
    let mut ops = Vec::new();
    let mut buf = BytesMut::new();

    if version >= 1 {
        put_i32(&mut buf, response.throttle_time_ms);
    }
    if version >= 7 {
        put_i16(&mut buf, response.error_code);
        put_i32(&mut buf, response.session_id);
    }

    put_array_len(&mut buf, response.responses.len(), flex);
    for topic in &response.responses {
        encode_fetch_topic(&mut buf, &mut ops, topic, version, flex)?;
    }

    if flex {
        let mut tagged = WriteTaggedFields::new();
        if !crabka_protocol::codegen_helpers::is_default(&response.node_endpoints) {
            let node_endpoints = &response.node_endpoints;
            let payload = encode_to_bytes(
                crabka_protocol::primitives::array::array_len_prefix_len(
                    node_endpoints.len(),
                    flex,
                ) + node_endpoints
                    .iter()
                    .map(|endpoint| endpoint.encoded_len(version))
                    .sum::<usize>(),
                |bytes| {
                    put_array_len(bytes, node_endpoints.len(), flex);
                    for endpoint in node_endpoints {
                        endpoint.encode(bytes, version)?;
                    }
                    Ok(())
                },
            );
            tagged.add(0, payload);
        }
        tagged.write(&mut buf, &response.unknown_tagged_fields);
    }

    flush_fetch_inline(&mut buf, &mut ops);
    Ok(ops)
}

fn encode_fetch_topic(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    topic: &FetchableTopicResponse,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    if (0..=12).contains(&version) {
        if flex {
            put_compact_string(buf, &topic.topic);
        } else {
            put_string(buf, &topic.topic);
        }
    }
    if version >= 13 {
        put_uuid(buf, topic.topic_id);
    }

    put_array_len(buf, topic.partitions.len(), flex);
    for partition in &topic.partitions {
        encode_fetch_partition(buf, ops, partition, version, flex)?;
    }
    if flex {
        WriteTaggedFields::new().write(buf, &topic.unknown_tagged_fields);
    }
    Ok(())
}

fn encode_fetch_partition(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    partition: &PartitionData,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    put_i32(buf, partition.partition_index);
    put_i16(buf, partition.error_code);
    put_i64(buf, partition.high_watermark);
    if version >= 4 {
        put_i64(buf, partition.last_stable_offset);
    }
    if version >= 5 {
        put_i64(buf, partition.log_start_offset);
    }
    if version >= 4 {
        put_nullable_array_len(
            buf,
            partition.aborted_transactions.as_ref().map(Vec::len),
            flex,
        );
        if let Some(aborted_transactions) = &partition.aborted_transactions {
            for transaction in aborted_transactions {
                transaction.encode(buf, version)?;
            }
        }
    }
    if version >= 11 {
        put_i32(buf, partition.preferred_read_replica);
    }

    encode_records_prefix(buf, ops, partition.records.as_ref(), flex)?;

    if flex {
        let mut tagged = WriteTaggedFields::new();
        if !crabka_protocol::codegen_helpers::is_default(&partition.diverging_epoch) {
            tagged.add(
                0,
                encode_to_bytes(partition.diverging_epoch.encoded_len(version), |bytes| {
                    partition.diverging_epoch.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        if !crabka_protocol::codegen_helpers::is_default(&partition.current_leader) {
            tagged.add(
                1,
                encode_to_bytes(partition.current_leader.encoded_len(version), |bytes| {
                    partition.current_leader.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        if !crabka_protocol::codegen_helpers::is_default(&partition.snapshot_id) {
            tagged.add(
                2,
                encode_to_bytes(partition.snapshot_id.encoded_len(version), |bytes| {
                    partition.snapshot_id.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        tagged.write(buf, &partition.unknown_tagged_fields);
    }

    Ok(())
}

fn encode_records_prefix(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    records: Option<&RecordsPayload>,
    flex: bool,
) -> Result<(), ProtocolError> {
    let Some(payload) = records else {
        if flex {
            crabka_protocol::primitives::varint::put_uvarint(buf, 0);
        } else {
            put_i32(buf, -1);
        }
        return Ok(());
    };

    let payload_len = payload.payload_len();
    if flex {
        let prefixed_len = u32::try_from(payload_len + 1)
            .map_err(|_| ProtocolError::InvalidValue("records too large for compact len"))?;
        crabka_protocol::primitives::varint::put_uvarint(buf, prefixed_len);
    } else {
        let records_len = i32::try_from(payload_len)
            .map_err(|_| ProtocolError::InvalidValue("records too large for i32 len"))?;
        put_i32(buf, records_len);
    }
    flush_fetch_inline(buf, ops);
    ops.push(FetchWriteOp::Records(payload.clone()));
    Ok(())
}

fn flush_fetch_inline(buf: &mut BytesMut, ops: &mut Vec<FetchWriteOp>) {
    if buf.is_empty() {
        return;
    }
    ops.push(FetchWriteOp::Inline(buf.split().freeze()));
}

/// Build the ordered [`WriteOp`] plan for a v4+ fetch response. The first
/// inline op holds the leading frame-length prefix and the correlation header.
///
/// Byte-exactness: the concatenation of every op's bytes equals exactly what
/// `encode_response(api_key=1, correlation_id, body_flexible,
/// &encode_fetch_response(resp))` would produce, because:
///   * the frame length == 4-byte big-endian `u32` of `(header_len + body_len)`,
///   * the header == `correlation_id`, plus an empty tagged byte iff `body_flexible`,
///   * the body ops come straight from [`FetchResponse::write_plan`], whose
///     concatenation is byte-identical to `FetchResponse::encode`. The
///     protocol-crate golden tests prove that.
///
/// `resolve_records` decides how the writer emits each records segment. For
/// the portable C path see [`resolve_records_inline`]. Increment D supplies a
/// resolver that emits `WriteOp::File` on Linux plaintext.
pub fn build_fetch_plan<F>(
    resp: &FetchResponse,
    version: i16,
    correlation_id: i32,
    body_flexible: bool,
    max_frame_bytes: usize,
    mut resolve_records: F,
) -> Result<Vec<WriteOp>, BrokerError>
where
    F: FnMut(&RecordsPayload) -> Result<Vec<WriteOp>, BrokerError>,
{
    debug_assert!(
        version >= 4,
        "build_fetch_plan requires the canonical v4+ codec"
    );
    // The response header is v1 (a trailing empty tagged-fields byte) iff the
    // body is flexible. The `encode_response` exception for ApiVersions
    // (api_key 18) never applies here — this is always Fetch (api_key 1).
    let header_v1 = response_header_v1(ApiKey::Fetch as i16, body_flexible);
    let header_len = response_header_len(ApiKey::Fetch as i16, body_flexible);

    let proto_plan = fetch_response_write_plan(resp, version)?;
    let body_len: usize = proto_plan.iter().map(FetchWriteOp::len).sum();
    let frame_body_len = header_len + body_len;
    codec::validate_frame_length(frame_body_len, max_frame_bytes)?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(proto_plan.len() + 1);

    // First inline op: 4-byte frame length + correlation header.
    let mut head = BytesMut::with_capacity(4 + header_len);
    head.put_u32(u32::try_from(frame_body_len).expect("checked against configured frame maximum"));
    head.put_i32(correlation_id);
    if header_v1 {
        head.put_u8(0); // empty response-header tagged fields
    }
    ops.push(WriteOp::Inline(head.freeze()));

    for op in proto_plan {
        match op {
            FetchWriteOp::Inline(b) => ops.push(WriteOp::Inline(b)),
            FetchWriteOp::Records(payload) => {
                ops.extend(resolve_records(&payload)?);
            }
        }
    }
    Ok(ops)
}

/// Portable (Increment C) records resolver: emit the records payload as a
/// single inline segment. For `RecordsPayload::Raw` this function hands the
/// verbatim `.log` `Bytes` to the socket directly. That is a refcounted view
/// and copies no records bytes. For parsed and legacy payloads it encodes them
/// into a fresh buffer, which is the rare non-passthrough path. For a
/// `FileRegions` payload, the TLS and non-Linux fallback, it `pread`s the
/// regions into one buffer.
pub fn resolve_records_inline(payload: &RecordsPayload) -> Result<Vec<WriteOp>, BrokerError> {
    let bytes = match payload {
        // `Raw`/`Legacy` are already verbatim wire bytes — share the `Bytes`.
        RecordsPayload::Raw(b) | RecordsPayload::Legacy(b) => b.clone(),
        // Parsed batches must be encoded; rare on the fetch path.
        RecordsPayload::V2(_) => {
            let mut buf = BytesMut::with_capacity(payload.payload_len());
            payload
                .encode_to(&mut buf)
                .map_err(|e| BrokerError::Io(std::io::Error::other(e.to_string())))?;
            buf.freeze()
        }
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        RecordsPayload::FileRegions(_) => {
            // TLS / non-sendfile fallback for a FileRegions payload: pread into a
            // buffer (byte-identical to the sendfile'd region).
            let mut buf = BytesMut::with_capacity(payload.payload_len());
            payload
                .encode_to(&mut buf)
                .map_err(|e| BrokerError::Io(std::io::Error::other(e.to_string())))?;
            buf.freeze()
        }
    };
    Ok(vec![WriteOp::Inline(bytes)])
}

crate::sendfile_cfg! {
    /// Plaintext-sendfile (Increments D + E) records resolver: emit each
    /// `FileRegion` of a `FileRegions` payload as its own [`WriteOp::File`],
    /// one per contributing segment, for the kernel `sendfile` drain. Every
    /// other payload kind defers to [`resolve_records_inline`], as does a
    /// `FileRegions` payload that arrives here on a non-sendfile path. This
    /// function compiles on the SENDFILE alias (Linux + Apple +
    /// FreeBSD/DragonFly).
    pub fn resolve_records_sendfile(payload: &RecordsPayload) -> Result<Vec<WriteOp>, BrokerError> {
        match payload {
            RecordsPayload::FileRegions(regions) => {
                Ok(regions.iter().cloned().map(WriteOp::File).collect())
            }
            _ => resolve_records_inline(payload),
        }
    }
}

/// A byte sink that can also drain a segment-file-backed records region with
/// the most efficient mechanism available to it.
///
/// On a SENDFILE-alias platform (Linux + Apple + FreeBSD/DragonFly) a
/// plaintext `TcpStream` exposes its underlying socket for the
/// readiness-driven `sendfile` loop. Every other stream returns `None`,
/// including TLS, which encrypts in userspace. The drainer then falls back to
/// a buffered `pread` + `write_all` that produces identical wire bytes.
/// Windows has no safe `sendfile` or `TransmitFile`, so the
/// `tcp_for_sendfile` method is compiled out and sendfile is never used.
pub trait SendfileSink {
    /// `true` when this stream can serve a records region with the kernel
    /// `sendfile(2)`, that is, a plaintext `TcpStream` on a SENDFILE-alias
    /// platform. Always `false` on TLS and on Windows. The fetch handler uses
    /// this to decide whether to emit `RecordsPayload::FileRegions` at all.
    fn is_sendfile_capable(&self) -> bool;

    crate::sendfile_cfg! {
        /// Borrow the underlying `TcpStream` for readiness-driven `sendfile`,
        /// when this stream *is* a plaintext `TcpStream`. `None` for TLS.
        /// Present only on SENDFILE-alias platforms (Linux + Apple +
        /// FreeBSD/DragonFly). Windows has no compatible safe `sendfile`.
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream>;
    }
}

impl SendfileSink for tokio::net::TcpStream {
    fn is_sendfile_capable(&self) -> bool {
        // True on every SENDFILE-alias platform; false on Windows.
        cfg!(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))
    }
    crate::sendfile_cfg! {
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
            Some(self)
        }
    }
}

impl SendfileSink for tokio_rustls::server::TlsStream<tokio::net::TcpStream> {
    // Userspace TLS: rustls encrypts in userspace, so file bytes must pass
    // through the rustls buffer — there is no kernel file→socket path. This is
    // the non-kTLS fallback (the broker's `ktls_enabled` startup probe returned
    // false, or the kernel lacks the `tls` module): the connection is served
    // over a plain `TlsStream` and the fetch path falls back to pread+write_all.
    fn is_sendfile_capable(&self) -> bool {
        false
    }
    crate::sendfile_cfg! {
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
            None
        }
    }
}

// Linux kTLS (Increment F): a `KtlsStream` is just a TCP socket whose TLS
// record encryption is offloaded to the kernel. `sendfile(2)` onto the inner
// `TcpStream` makes the kernel encrypt the page-cache pages into TLS records on
// the way out — restoring zero-copy fetch on encrypted connections. The drainer
// (`write_fetch_plan`/`sendfile_region`) treats it identically to a plaintext
// `TcpStream`; only the encrypt locus moves kernel-side, so the TLS record
// stream a client decrypts is byte-identical to the userspace rustls path.
#[cfg(target_os = "linux")]
impl SendfileSink for ktls::KtlsStream<tokio::net::TcpStream> {
    fn is_sendfile_capable(&self) -> bool {
        true
    }
    fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
        Some(self.get_ref())
    }
}

/// Drain a fetch write-plan to `stream`, write each op in order, then flush.
///
/// The caller MUST have flushed any pending `Framed` codec output first, so
/// the bytes do not interleave with the codec's write buffer. Inline ops use
/// `write_all`. File ops use `sendfile` when the stream is a Linux plaintext
/// `TcpStream`, and a buffered `pread` + `write_all` fallback otherwise.
pub async fn write_fetch_plan<S>(stream: &mut S, ops: Vec<WriteOp>) -> Result<(), BrokerError>
where
    S: AsyncWrite + SendfileSink + Unpin,
{
    for op in ops {
        match op {
            WriteOp::Inline(b) => {
                stream.write_all(&b).await.map_err(BrokerError::Io)?;
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            WriteOp::File(region) => {
                drain_file_region(stream, &region).await?;
            }
        }
    }
    stream.flush().await.map_err(BrokerError::Io)?;
    Ok(())
}

crate::sendfile_cfg! {
    /// Positioned, full read of a `FileRegion` into `dst`, in a loop over
    /// short reads. `dst` must be exactly `region.len` bytes. This is the TLS
    /// and non-sendfile fallback for `WriteOp::File`. `read_at` (`FileExt`) is
    /// portable across every SENDFILE-alias unix.
    fn read_region_exact(
        region: &crabka_protocol::records::FileRegion,
        dst: &mut [u8],
    ) -> Result<(), BrokerError> {
        use std::os::unix::fs::FileExt;
        debug_assert_eq!(dst.len(), region.len);
        let mut filled = 0usize;
        let mut offset = region.offset;
        while filled < dst.len() {
            match region.file.read_at(&mut dst[filled..], offset) {
                Ok(0) => {
                    return Err(BrokerError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "FileRegion read hit EOF before len bytes",
                    )));
                }
                Ok(n) => {
                    filled += n;
                    offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(BrokerError::Io(e)),
            }
        }
        Ok(())
    }

    /// Drain one `FileRegion` to the socket. This method uses the kernel
    /// `sendfile(2)` when the stream is a plaintext `TcpStream` on a
    /// SENDFILE-alias platform. On TLS it falls back to a buffered `pread` +
    /// `write_all` that produces identical wire bytes.
    async fn drain_file_region<S>(
        stream: &mut S,
        region: &crabka_protocol::records::FileRegion,
    ) -> Result<(), BrokerError>
    where
        S: AsyncWrite + SendfileSink + Unpin,
    {
        if stream.tcp_for_sendfile().is_some() {
            // Re-borrow immutably for the readiness loop. `writable()`/`try_io()`
            // take `&self`, so this never conflicts with the (released) `&mut`.
            let tcp = stream
                .tcp_for_sendfile()
                .expect("checked Some on the line above");
            sendfile_region(tcp, region).await
        } else {
            // TLS fallback: pread the region into a buffer and write it.
            let mut buf = BytesMut::zeroed(region.len);
            read_region_exact(region, &mut buf)?;
            stream.write_all(&buf).await.map_err(BrokerError::Io)?;
            Ok(())
        }
    }
}

crate::sendfile_cfg! {
    /// `sendfile(2)` a `FileRegion` to a plaintext `TcpStream`, in a loop over
    /// partial writes and `EAGAIN`.
    ///
    /// Every SENDFILE-alias platform **shares** the readiness loop. The socket
    /// is non-blocking under tokio, so on a full socket buffer the syscall
    /// reports `EAGAIN`/`WouldBlock`. The loop then awaits `tcp.writable()`
    /// and retries. `TcpStream::try_io` clears the readiness flag correctly on
    /// `WouldBlock`, so this needs no `spawn_blocking` and no second `AsyncFd`
    /// over the fd.
    ///
    /// This method tracks its own `sent_total` cursor and computes the
    /// absolute file offset for each attempt as `region.offset + sent_total`.
    /// It never touches the file's own cursor, and concurrent reads of the
    /// same `Arc<File>` stay unaffected. Only the single per-OS syscall
    /// attempt ([`sendfile_once`]) is cfg-selected. Everything around it is
    /// identical on Linux and on Apple/BSD.
    async fn sendfile_region(
        tcp: &tokio::net::TcpStream,
        region: &crabka_protocol::records::FileRegion,
    ) -> Result<(), BrokerError> {
        use std::io::ErrorKind;
        use std::os::fd::{AsFd, BorrowedFd};

        let in_fd: BorrowedFd<'_> = region.file.as_fd();
        // `TcpStream: AsFd` — borrow the socket fd safely (no `unsafe`/`borrow_raw`).
        let out_fd: BorrowedFd<'_> = tcp.as_fd();

        let mut sent_total: usize = 0;

        while sent_total < region.len {
            // Wait for the socket to be writable, then attempt one sendfile. If
            // the kernel reports it would block (with no forward progress),
            // `try_io` returns WouldBlock and we loop back to `writable()`.
            tcp.writable().await.map_err(BrokerError::Io)?;
            let offset = region.offset + sent_total as u64;
            let remaining = region.len - sent_total;
            let res = tcp.try_io(tokio::io::Interest::WRITABLE, || {
                sendfile_once(out_fd, in_fd, offset, remaining)
            });
            match res {
                Ok(0) => {
                    // The syscall reported success with zero bytes while bytes
                    // still remain: the source file is shorter than expected
                    // (truncated mid-send). The `Arc<File>` should prevent this;
                    // treat as an I/O error so the connection closes rather than
                    // emitting a short frame.
                    return Err(BrokerError::Io(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "sendfile returned 0 before region fully sent",
                    )));
                }
                Ok(n) => {
                    sent_total += n;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // Not writable yet (true would-block: zero bytes moved); loop
                    // and re-await readiness.
                }
                Err(e) => return Err(BrokerError::Io(e)),
            }
        }
        Ok(())
    }
}

/// One non-blocking `sendfile(2)` attempt. It returns the bytes transferred on
/// this call. A true would-block, with zero forward progress, surfaces as
/// `ErrorKind::WouldBlock` so the shared readiness loop re-arms. Any positive
/// transfer returns `Ok(n)`, even when the kernel also signalled `EAGAIN`.
///
/// **Linux** (`rustix`): `sendfile(out, in, Some(&mut offset), count)` returns
/// the count and mutates `offset` in place. On `EAGAIN` it returns `Err`. The
/// kernel does not report a partial count in `errno`, so `Err(EAGAIN)` always
/// means zero bytes on this call. This function maps it straight to
/// `WouldBlock`.
#[cfg(target_os = "linux")]
fn sendfile_once(
    out_fd: std::os::fd::BorrowedFd<'_>,
    in_fd: std::os::fd::BorrowedFd<'_>,
    offset: u64,
    count: usize,
) -> std::io::Result<usize> {
    let mut off = offset;
    rustix::fs::sendfile(out_fd, in_fd, Some(&mut off), count).map_err(std::io::Error::from)
}

/// **Apple / FreeBSD / `DragonFly`** (`nix`): the BSD-family `sendfile` returns
/// `(nix::Result<()>, off_t)`, where the `off_t` is the bytes transferred on
/// this call. That count is **valid even on `Err(EAGAIN)`**. This is the
/// correctness landmine: on these platforms `EAGAIN` with `n > 0` is *forward
/// progress*, not would-block. So this function does the following:
///
/// * `Ok(())` → return `Ok(n)`. The send was full or partial, and the loop
///   advances by `n`.
/// * `Err(EAGAIN)` with `n>0` → return `Ok(n)`. This counts the progress, and
///   the loop advances and re-arms readiness for the rest.
/// * `Err(EAGAIN)` with `n==0` → return `Err(WouldBlock)`, a real would-block.
/// * any other `Err` → propagate as a hard I/O error.
///
/// `count` is always `Some(region_remaining)` and never `None` or 0, which
/// would mean "to EOF", so this function never overshoots into the next batch.
/// The header and trailer `hdtr` slices are `None`, because the frame metadata
/// is a separate `WriteOp::Inline`, exactly as on Linux.
///
/// NOTE: this arm is compile-reasoned only. The Windows/WSL toolchains used
/// here do not build or run it. It needs a macOS or FreeBSD CI runner to
/// verify the syscall semantics and the byte-exact wire output.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
fn sendfile_once(
    out_sock: std::os::fd::BorrowedFd<'_>,
    in_fd: std::os::fd::BorrowedFd<'_>,
    offset: u64,
    count: usize,
) -> std::io::Result<usize> {
    use nix::errno::Errno;

    // `off_t` is the kernel's signed file-offset type; the byte ranges we send
    // are bounded by the segment size and always fit. Saturate defensively
    // rather than wrap if an offset ever exceeded `off_t::MAX`.
    let off = nix::libc::off_t::try_from(offset).unwrap_or(nix::libc::off_t::MAX);

    // The `count` parameter's element type differs by platform: macOS/iOS take
    // `Option<off_t>`, FreeBSD/DragonFly take `Option<usize>`. The
    // `count_arg` shim normalizes our `usize remaining` to the right type. We
    // never pass `None` (which would mean "send to EOF" and could overshoot the
    // current batch into the next one in the same `.log` file).
    let (result, sent) = bsd_sendfile(in_fd, out_sock, off, count);

    let n = usize::try_from(sent).unwrap_or(0);
    match result {
        // Fully/partially transferred without error.
        Ok(()) => Ok(n),
        // EAGAIN/EWOULDBLOCK: on BSD-family this can accompany real forward
        // progress (n > 0). Count the progress; only a zero-progress EAGAIN is a
        // true would-block that the readiness loop must wait on.
        Err(Errno::EAGAIN) => {
            if n > 0 {
                Ok(n)
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        }
        // EINTR with progress is also forward progress; without progress, retry
        // is harmless — surface as WouldBlock so the loop re-arms and re-issues.
        Err(Errno::EINTR) if n > 0 => Ok(n),
        Err(Errno::EINTR) => Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
        Err(e) => Err(std::io::Error::from_raw_os_error(e as i32)),
    }
}

/// Platform shim over the per-OS BSD-family `nix::sys::sendfile::sendfile`
/// signatures. macOS uses `Option<off_t>` for `count` and no flags. FreeBSD
/// also takes `SfFlags` and a readahead hint. `DragonFly` takes neither, but
/// uses `Option<usize>`. Returns `(nix::Result<()>, off_t bytes_sent)`.
///
/// Compile-reasoned only, because there is no macOS or BSD toolchain here. It
/// needs CI verification.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos"
))]
fn bsd_sendfile(
    in_fd: std::os::fd::BorrowedFd<'_>,
    out_sock: std::os::fd::BorrowedFd<'_>,
    offset: nix::libc::off_t,
    count: usize,
) -> (nix::Result<()>, nix::libc::off_t) {
    // macOS/iOS: count is `Option<off_t>`; no header/trailer; no flags.
    let count = Some(nix::libc::off_t::try_from(count).unwrap_or(nix::libc::off_t::MAX));
    nix::sys::sendfile::sendfile(in_fd, out_sock, offset, count, None, None)
}

#[cfg(target_os = "freebsd")]
fn bsd_sendfile(
    in_fd: std::os::fd::BorrowedFd<'_>,
    out_sock: std::os::fd::BorrowedFd<'_>,
    offset: nix::libc::off_t,
    count: usize,
) -> (nix::Result<()>, nix::libc::off_t) {
    // FreeBSD: count is `Option<usize>`; additional `SfFlags` + readahead args.
    nix::sys::sendfile::sendfile(
        in_fd,
        out_sock,
        offset,
        Some(count),
        None,
        None,
        nix::sys::sendfile::SfFlags::empty(),
        0,
    )
}

#[cfg(target_os = "dragonfly")]
fn bsd_sendfile(
    in_fd: std::os::fd::BorrowedFd<'_>,
    out_sock: std::os::fd::BorrowedFd<'_>,
    offset: nix::libc::off_t,
    count: usize,
) -> (nix::Result<()>, nix::libc::off_t) {
    // DragonFly: count is `Option<usize>`; no flags/readahead.
    nix::sys::sendfile::sendfile(in_fd, out_sock, offset, Some(count), None, None)
}

#[cfg(test)]
mod tests {
    use crabka_protocol::{
        Encode,
        owned::fetch_response::{FetchableTopicResponse, PartitionData},
        records::{Record, RecordBatch, RecordsPayload},
    };

    use super::*;

    const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

    fn raw_batch(base: i64) -> Bytes {
        let rb = RecordBatch {
            base_offset: base,
            records: vec![Record {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"value-payload")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        buf.freeze()
    }

    /// Extract the bytes of an `Inline` op. It panics on a `File` op. This
    /// avoids a `match`/`let-else` that is infallible on Windows, with one
    /// variant, but refutable on SENDFILE-alias platforms, with two variants.
    /// Clippy stays happy on both.
    fn inline_bytes(op: &WriteOp) -> &Bytes {
        match op {
            WriteOp::Inline(b) => b,
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            WriteOp::File(_) => panic!("expected an inline op"),
        }
    }

    fn sample_response(version: i16) -> FetchResponse {
        let p0 = PartitionData {
            partition_index: 0,
            high_watermark: 1,
            last_stable_offset: 1,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(raw_batch(0))),
            ..PartitionData::default()
        };
        let p1 = PartitionData {
            partition_index: 1,
            high_watermark: 2,
            last_stable_offset: 2,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(raw_batch(1))),
            ..PartitionData::default()
        };
        FetchResponse {
            throttle_time_ms: 0,
            session_id: 7,
            responses: vec![FetchableTopicResponse {
                topic: if version <= 12 {
                    "t".to_string()
                } else {
                    String::new()
                },
                topic_id: if version >= 13 {
                    crabka_protocol::primitives::uuid::Uuid([5u8; 16])
                } else {
                    crabka_protocol::primitives::uuid::Uuid([0u8; 16])
                },
                partitions: vec![p0, p1],
                ..FetchableTopicResponse::default()
            }],
            ..FetchResponse::default()
        }
    }

    /// The broker-level golden test: the full framed bytes that
    /// `build_fetch_plan` produces, which are the length prefix, the
    /// correlation header, and the body, must equal the bytes from the
    /// `encode_response(encode_fetch_response(..))` path, for both
    /// non-flexible and flexible versions.
    #[test]
    fn build_fetch_plan_matches_legacy_encode_path() {
        for version in [4i16, 7, 11, 12, 13, 16, 18] {
            let resp = sample_response(version);
            let correlation_id = 0x1234_5678;
            let body_flexible = version >= 12;

            // New path: assemble the plan bytes.
            let ops = build_fetch_plan(
                &resp,
                version,
                correlation_id,
                body_flexible,
                DEFAULT_MAX_FRAME_BYTES,
                resolve_records_inline,
            )
            .unwrap();
            let mut new_bytes = BytesMut::new();
            for op in &ops {
                match op {
                    WriteOp::Inline(b) => new_bytes.extend_from_slice(b),
                    #[cfg(any(
                        target_os = "linux",
                        target_os = "macos",
                        target_os = "ios",
                        target_os = "tvos",
                        target_os = "watchos",
                        target_os = "freebsd",
                        target_os = "dragonfly",
                    ))]
                    WriteOp::File(_) => unreachable!("inline resolver emits no File ops"),
                }
            }

            // Old path: encode the body, then the response header, then frame.
            let mut body = BytesMut::new();
            resp.encode(&mut body, version).unwrap();
            let header_v1 = response_header_v1(ApiKey::Fetch as i16, body_flexible);
            let header_len = response_header_len(ApiKey::Fetch as i16, body_flexible);
            let frame_body_len = header_len + body.len();
            let mut old_bytes = BytesMut::new();
            old_bytes.put_u32(u32::try_from(frame_body_len).unwrap());
            old_bytes.put_i32(correlation_id);
            if header_v1 {
                old_bytes.put_u8(0);
            }
            old_bytes.extend_from_slice(&body);

            assert_eq!(
                &new_bytes[..],
                &old_bytes[..],
                "plan != legacy encode at version {version}"
            );
        }
    }

    #[test]
    fn plan_total_len_matches_frame_prefix() {
        // The 4-byte frame prefix the writer emits must equal the actual bytes
        // following it (header + body). Off-by-one here corrupts every frame.
        for version in [4i16, 12, 18] {
            let resp = sample_response(version);
            let ops = build_fetch_plan(
                &resp,
                version,
                1,
                version >= 12,
                DEFAULT_MAX_FRAME_BYTES,
                resolve_records_inline,
            )
            .unwrap();
            // First op is [u32 len][header]; the declared length must equal the
            // sum of the remaining bytes of op0 (the header) + all later ops.
            let head = inline_bytes(&ops[0]);
            let declared = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
            let header_after_len = head.len() - 4;
            let tail_len: usize = ops[1..].iter().map(WriteOp::len).sum();
            assert_eq!(declared, header_after_len + tail_len);
        }
    }

    #[test]
    fn build_fetch_plan_honors_nondefault_max_frame_length() {
        let response = sample_response(12);
        let unconstrained =
            build_fetch_plan(&response, 12, 1, true, usize::MAX, resolve_records_inline)
                .expect("unconstrained plan");
        let head = inline_bytes(&unconstrained[0]);
        let frame_body_len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;

        assert!(
            build_fetch_plan(
                &response,
                12,
                1,
                true,
                frame_body_len,
                resolve_records_inline,
            )
            .is_ok(),
            "a frame equal to the configured maximum must be accepted"
        );
        assert!(
            build_fetch_plan(
                &response,
                12,
                1,
                true,
                frame_body_len - 1,
                resolve_records_inline,
            )
            .is_err(),
            "a frame above the configured maximum must be rejected"
        );
    }

    // ─── Increment D/E (cross-platform sendfile) tests ────────────────────
    // Compiled on every SENDFILE-alias platform (Linux + Apple + FreeBSD/
    // DragonFly). The plan-shape + pread-fallback tests are pure userspace and
    // portable; the loopback-TCP roundtrip exercises the real readiness +
    // partial-write loop. The kTLS roundtrip below stays Linux-only (kTLS is a
    // Linux-only dependency).
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    mod sendfile_tests {
        use std::{io::Write as _, sync::Arc};

        use crabka_protocol::records::FileRegion;

        use super::*;

        /// Write `bytes` to a temp file and return a single-region
        /// `RecordsPayload::FileRegions` that covers the whole file from
        /// offset 0.
        fn file_payload(bytes: &[u8]) -> (tempfile::NamedTempFile, RecordsPayload) {
            let mut tf = tempfile::NamedTempFile::new().unwrap();
            tf.write_all(bytes).unwrap();
            tf.flush().unwrap();
            let file = Arc::new(tf.reopen().unwrap());
            let payload = RecordsPayload::FileRegions(vec![FileRegion {
                file,
                offset: 0,
                len: bytes.len(),
            }]);
            (tf, payload)
        }

        /// The Increment-D wire invariant: a `FileRegions` payload through the
        /// sendfile resolver produces the SAME framed wire bytes as the
        /// equivalent `Raw` payload through the inline resolver. Only the op
        /// kinds differ, File against Inline. The records bytes the broker
        /// emits are identical for both the sendfile path and the copy path.
        #[test]
        fn sendfile_plan_wire_bytes_equal_raw_plan() {
            for version in [4i16, 11, 12, 18] {
                // Records bytes large enough to be realistic.
                let records = {
                    let mut b = BytesMut::new();
                    b.extend_from_slice(&raw_batch(0));
                    b.extend_from_slice(&raw_batch(1));
                    b.freeze()
                };
                let (_tf, file_payload) = file_payload(&records);

                let raw_resp = FetchResponse {
                    session_id: 1,
                    responses: vec![FetchableTopicResponse {
                        topic: if version <= 12 {
                            "t".into()
                        } else {
                            String::new()
                        },
                        partitions: vec![PartitionData {
                            partition_index: 0,
                            high_watermark: 2,
                            last_stable_offset: 2,
                            log_start_offset: 0,
                            records: Some(RecordsPayload::Raw(records.clone())),
                            ..PartitionData::default()
                        }],
                        ..FetchableTopicResponse::default()
                    }],
                    ..FetchResponse::default()
                };
                let mut file_resp = raw_resp.clone();
                file_resp.responses[0].partitions[0].records = Some(file_payload);

                let raw_ops = build_fetch_plan(
                    &raw_resp,
                    version,
                    9,
                    version >= 12,
                    DEFAULT_MAX_FRAME_BYTES,
                    resolve_records_inline,
                )
                .unwrap();
                let file_ops = build_fetch_plan(
                    &file_resp,
                    version,
                    9,
                    version >= 12,
                    DEFAULT_MAX_FRAME_BYTES,
                    resolve_records_sendfile,
                )
                .unwrap();

                // The file plan must actually contain a File op (zero-copy).
                assert!(
                    file_ops.iter().any(|o| matches!(o, WriteOp::File(_))),
                    "sendfile resolver must emit a File op at v{version}"
                );

                // Resolve both plans to bytes (pread the file ops) and compare.
                let raw_bytes = resolve_ops_to_bytes(&raw_ops);
                let file_bytes = resolve_ops_to_bytes(&file_ops);
                assert_eq!(
                    raw_bytes, file_bytes,
                    "sendfile plan wire bytes must equal raw plan at v{version}"
                );
            }
        }

        /// Resolve a plan to bytes and read File ops out of their backing
        /// file. This mirrors what the sendfile drain transmits and what the
        /// TLS pread fallback copies.
        fn resolve_ops_to_bytes(ops: &[WriteOp]) -> Vec<u8> {
            use std::os::unix::fs::FileExt;
            let mut out = Vec::new();
            for op in ops {
                match op {
                    WriteOp::Inline(b) => out.extend_from_slice(b),
                    WriteOp::File(region) => {
                        let mut buf = vec![0u8; region.len];
                        let mut filled = 0;
                        let mut off = region.offset;
                        while filled < buf.len() {
                            let n = region.file.read_at(&mut buf[filled..], off).unwrap();
                            assert!(n > 0);
                            filled += n;
                            off += n as u64;
                        }
                        out.extend_from_slice(&buf);
                    }
                }
            }
            out
        }

        /// The TLS / non-sendfile fallback: `resolve_records_inline` on a
        /// `FileRegions` payload preads the regions into one inline op whose
        /// bytes equal the file contents.
        #[test]
        fn inline_fallback_preads_file_regions() {
            let records = {
                let mut b = BytesMut::new();
                b.extend_from_slice(&raw_batch(3));
                b.extend_from_slice(&raw_batch(4));
                b.freeze()
            };
            let (_tf, payload) = file_payload(&records);
            let ops = resolve_records_inline(&payload).unwrap();
            assert_eq!(ops.len(), 1);
            let WriteOp::Inline(ref b) = ops[0] else {
                panic!("fallback must produce an inline op");
            };
            assert_eq!(&b[..], &records[..]);
        }

        /// End-to-end `sendfile` over a real loopback TCP socket: the bytes
        /// the client reads must equal the file region. The test drives the
        /// real readiness and partial-write loop in `write_fetch_plan`.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn sendfile_roundtrip_over_tcp_is_byte_exact() {
            use tokio::{
                io::AsyncReadExt,
                net::{TcpListener, TcpStream},
            };

            // A payload comfortably larger than a typical socket buffer so the
            // sendfile loop must iterate across several partial writes.
            let mut records = Vec::new();
            for i in 0..4000u32 {
                records.extend_from_slice(&i.to_le_bytes());
            }
            let records = Bytes::from(records);
            let (_tf, payload) = file_payload(&records);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let expected = records.clone();
            let client = tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                let mut got = vec![0u8; expected.len()];
                stream.read_exact(&mut got).await.unwrap();
                assert_eq!(got, &expected[..], "sendfile'd bytes must match file");
            });

            let (mut server, _) = listener.accept().await.unwrap();
            // Shrink the send buffer to force partial sendfile writes.
            {
                use socket2::SockRef;
                let sr = SockRef::from(&server);
                let _ = sr.set_send_buffer_size(8 * 1024);
            }
            let ops = resolve_records_sendfile(&payload).unwrap();
            assert!(ops.iter().any(|o| matches!(o, WriteOp::File(_))));
            write_fetch_plan(&mut server, ops).await.unwrap();
            drop(server); // EOF for the client's read_exact tail
            client.await.unwrap();
        }

        /// Increment F end-to-end: `sendfile(2)` a `FileRegions` payload onto a
        /// `ktls::KtlsStream` (kernel-offloaded TLS) and assert the bytes a
        /// rustls TLS *client* decrypts are byte-identical to the file region.
        /// This proves that the kTLS path is wire-compatible: the kernel
        /// encrypts the same plaintext the userspace rustls path would have,
        /// so the client sees the same plaintext after decryption.
        ///
        /// The test skips, and does not fail, when the host kernel has no
        /// kTLS support, that is, when the `tls` module is not loaded or
        /// `CONFIG_TLS` is absent. The startup probe gates this exact
        /// condition in production, so a skip here mirrors a run of the
        /// fallback path.
        ///
        /// Linux-only: `ktls` is a Linux-only dependency, so this test is not
        /// compiled on the Apple/BSD members of the SENDFILE alias.
        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn ktls_sendfile_over_tls_is_byte_exact() {
            use std::sync::Arc;

            use tokio::{
                io::{AsyncReadExt, AsyncWriteExt},
                net::{TcpListener, TcpStream},
            };

            // The request the client sends before reading the response —
            // mirrors the real broker flow (client sends ApiVersions/Fetch, the
            // broker reads it via `Framed`, then writes the fetch response). It
            // also exercises the kTLS RX path on the server side.
            const REQ: &[u8] = b"fetch-request";

            let _ = rustls::crypto::ring::default_provider().install_default();

            // Records large enough to span several TLS records + partial writes.
            let mut records = Vec::new();
            for i in 0..8000u32 {
                records.extend_from_slice(&i.to_le_bytes());
            }
            let records = Bytes::from(records);
            let (_tf, payload) = file_payload(&records);

            // Throwaway self-signed cert (localhost SAN).
            let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
            let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            let cert = params.self_signed(&key).unwrap();
            let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
            let key_der = rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap();

            let mut server_cfg =
                rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_no_client_auth()
                    .with_single_cert(vec![cert_der.clone()], key_der)
                    .unwrap();
            server_cfg.enable_secret_extraction = true;
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert_der).unwrap();
            let client_cfg =
                rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_root_certificates(roots)
                    .with_no_client_auth();
            let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let expected = records.clone();
            let client = tokio::spawn(async move {
                let tcp = TcpStream::connect(addr).await.unwrap();
                let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                let mut tls = connector.connect(name, tcp).await.unwrap();
                tls.write_all(REQ).await.unwrap();
                tls.flush().await.unwrap();
                let mut got = vec![0u8; expected.len()];
                tls.read_exact(&mut got).await.unwrap();
                got
            });

            let (tcp, _) = listener.accept().await.unwrap();
            {
                use socket2::SockRef;
                let sr = SockRef::from(&tcp);
                let _ = sr.set_send_buffer_size(16 * 1024);
            }
            let tls = acceptor.accept(ktls::CorkStream::new(tcp)).await.unwrap();
            let mut ktls_stream = match ktls::config_ktls_server(tls).await {
                Ok(s) => s,
                Err(e) => {
                    // Kernel lacks kTLS support — the production startup probe
                    // would have returned false and the fallback path runs.
                    eprintln!(
                        "skipping ktls_sendfile_over_tls_is_byte_exact: kTLS unsupported on this host: {e}"
                    );
                    client.abort();
                    return;
                }
            };

            // Read the client's request through the KtlsStream first (kTLS RX
            // + the ktls crate's drained-bytes replay). The real broker always
            // reads the Fetch request before writing the response.
            let mut req = vec![0u8; REQ.len()];
            ktls_stream.read_exact(&mut req).await.unwrap();
            assert_eq!(req, REQ, "kTLS RX must deliver the request bytes intact");

            // The KtlsStream must report itself sendfile-capable, and the
            // resolver must emit a File op (true zero-copy over TLS).
            assert!(SendfileSink::is_sendfile_capable(&ktls_stream));
            let ops = resolve_records_sendfile(&payload).unwrap();
            assert!(ops.iter().any(|o| matches!(o, WriteOp::File(_))));

            // sendfile the file region onto the kTLS socket — the kernel
            // encrypts it into TLS records on the way out.
            write_fetch_plan(&mut ktls_stream, ops).await.unwrap();
            ktls_stream.flush().await.unwrap();
            drop(ktls_stream); // sends close_notify; EOF for the client tail

            let got = client.await.unwrap();
            assert_eq!(
                got,
                &records[..],
                "client-decrypted kTLS bytes must equal the file region (wire byte-exact)"
            );
        }
    }
}

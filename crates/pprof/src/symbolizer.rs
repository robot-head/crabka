//! Lazy native symbolization wrapper.

use std::{
    collections::HashMap,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, secs,
};
use object::{Object, ObjectSymbol};

use crate::{Frame, RawLocation, SymbolDb, SymbolSource};

/// Hard cap on debuginfo artifacts downloaded from a (potentially untrusted)
/// debuginfod server. `build_id` is attacker-controlled, so a malicious or
/// compromised server could otherwise stream an unbounded body and exhaust
/// memory. 512 MiB comfortably covers real debug objects while bounding the
/// blast radius.
const MAX_DEBUGINFO: ByteSize = mebibytes(512);

/// How long a debuginfod connection may take to establish. A connect timeout
/// bounds slow-connect denial of service ahead of the total request timeout.
const DEBUGINFOD_CONNECT_TIMEOUT: Time = secs(5);

/// How long a whole debuginfod request may take, connection included.
const DEBUGINFOD_REQUEST_TIMEOUT: Time = secs(10);

/// Returns `true` iff `build_id` is a valid debuginfod build-id: a non-empty
/// lowercase hex string. debuginfod build-ids are hex digests, so anything
/// containing `/`, `.`, `..`, uppercase, or other bytes is rejected before it
/// can be interpolated into a URL (SSRF / path-traversal defence).
fn is_valid_build_id(build_id: &str) -> bool {
    build_id.len() >= 2
        && build_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Recover a poisoned mutex instead of propagating the panic. A single panicked
/// worker must not permanently `DoS` the resolver, so we take ownership of the
/// inner guard and carry on.
fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Parse an untrusted ELF/DWARF blob with `object::File::parse`, catching any
/// panic the parser might trigger on a crafted artifact. Returns `Ok(())` only
/// when the bytes parse cleanly without panicking.
fn parse_object_guarded(bytes: &[u8]) -> Result<(), String> {
    std::panic::catch_unwind(|| {
        object::File::parse(bytes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    })
    .unwrap_or_else(|_| Err("panic while parsing object file".to_string()))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SymbolizeRequest {
    pub build_id: String,
    pub filename: String,
    pub address: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSymbol {
    pub function: String,
    pub file: String,
    pub line: i32,
}

pub trait NativeResolver: Send + Sync {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>>;
}

#[derive(Default)]
pub struct ChainedResolver {
    resolvers: Vec<Arc<dyn NativeResolver>>,
}

impl ChainedResolver {
    #[must_use]
    pub fn new(resolvers: Vec<Arc<dyn NativeResolver>>) -> Self {
        Self { resolvers }
    }
}

impl NativeResolver for ChainedResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        self.resolvers
            .iter()
            .find_map(|resolver| resolver.symbolize(request))
    }
}

#[derive(Clone, Debug)]
pub struct ObjectSymbolResolver {
    bytes: Arc<Vec<u8>>,
    path: Option<PathBuf>,
}

impl ObjectSymbolResolver {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        parse_object_guarded(bytes.as_slice())?;
        Ok(Self {
            bytes: Arc::new(bytes),
            path: None,
        })
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let bytes = std::fs::read(&path).map_err(|err| err.to_string())?;
        parse_object_guarded(bytes.as_slice())?;
        Ok(Self {
            bytes: Arc::new(bytes),
            path: Some(path),
        })
    }
}

#[derive(Default)]
pub struct FileSystemResolver {
    cache: Mutex<HashMap<String, Option<ObjectSymbolResolver>>>,
}

impl NativeResolver for FileSystemResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        let mut cache = lock_recover(&self.cache);
        let resolver = cache
            .entry(request.filename.clone())
            .or_insert_with(|| ObjectSymbolResolver::from_file(&request.filename).ok());
        resolver
            .as_ref()
            .and_then(|resolver| resolver.symbolize(request))
    }
}

pub struct DebuginfodResolver {
    base_urls: Vec<reqwest::Url>,
    client: reqwest::blocking::Client,
    cache: Mutex<HashMap<String, Option<ObjectSymbolResolver>>>,
    max_debuginfo: ByteSize,
}

impl DebuginfodResolver {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn new(base_urls: Vec<String>) -> Result<Self, String> {
        Self::with_max_debuginfo(base_urls, MAX_DEBUGINFO)
    }

    fn with_max_debuginfo(base_urls: Vec<String>, max_debuginfo: ByteSize) -> Result<Self, String> {
        let base_urls = base_urls
            .into_iter()
            .filter(|url| !url.trim().is_empty())
            .map(|url| reqwest::Url::parse(url.trim()).map_err(|err| err.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        if base_urls.is_empty() {
            return Err("at least one debuginfod base URL is required".to_string());
        }
        // Do not follow redirects: a redirect from a debuginfod server is a
        // vector for SSRF pivots (e.g. to internal hosts or 169.254.169.254).
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(DEBUGINFOD_CONNECT_TIMEOUT.to_std())
            .timeout(DEBUGINFOD_REQUEST_TIMEOUT.to_std())
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self {
            base_urls,
            client,
            cache: Mutex::new(HashMap::new()),
            max_debuginfo,
        })
    }

    /// Build the `<base>/buildid/<build_id>/debuginfo` URL by pushing path
    /// segments through the URL parser, so an attacker-controlled `build_id`
    /// cannot alter the host or escape the path. Returns `None` if the base URL
    /// cannot be a base (e.g. `mailto:`).
    fn build_url(base: &reqwest::Url, build_id: &str) -> Option<reqwest::Url> {
        let mut url = base.clone();
        {
            let mut segments = url.path_segments_mut().ok()?;
            // Drop any trailing empty segment from a base URL ending in '/'.
            segments.pop_if_empty();
            segments.push("buildid").push(build_id).push("debuginfo");
        }
        Some(url)
    }

    fn resolver_for_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        let mut cache = lock_recover(&self.cache);
        if let Some(cached) = cache.get(build_id) {
            return cached.clone();
        }
        let resolver = self.fetch_build_id(build_id);
        cache.insert(build_id.to_string(), resolver.clone());
        resolver
    }

    fn fetch_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        // `build_id` is attacker-controlled (it comes from an uploaded
        // profile's mapping). Validate it is a plain hex build-id before it is
        // used to construct any URL or issued in any request.
        if !is_valid_build_id(build_id) {
            return None;
        }
        for base_url in &self.base_urls {
            let Some(url) = Self::build_url(base_url, build_id) else {
                continue;
            };
            let Ok(response) = self.client.get(url).send() else {
                continue;
            };
            if !response.status().is_success() {
                continue;
            }
            // Reject artifacts whose advertised length already exceeds the cap,
            // then read the body with a hard byte ceiling so a server that
            // lies about (or omits) Content-Length still cannot exhaust memory.
            let cap = self.max_debuginfo.bytes_u64();
            if !content_length_within_cap(response.content_length(), cap) {
                continue;
            }
            let Some(bytes) = read_capped(response, cap) else {
                continue;
            };
            if let Ok(resolver) = ObjectSymbolResolver::from_bytes(bytes) {
                return Some(resolver);
            }
        }
        None
    }
}

/// Read an HTTP body into memory, aborting (returning `None`) the moment the
/// accumulated size would exceed `cap` bytes. Avoids the unbounded
/// `response.bytes()` allocation.
fn read_capped(mut response: reqwest::blocking::Response, cap: u64) -> Option<Vec<u8>> {
    read_capped_reader(&mut response, cap)
}

fn read_capped_reader(mut reader: impl Read, cap: u64) -> Option<Vec<u8>> {
    let cap_usize = usize::try_from(cap).unwrap_or(usize::MAX);
    let mut buf = Vec::new();
    let mut chunk = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        if buf.len().saturating_add(read) > cap_usize {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    Some(buf)
}

fn content_length_within_cap(content_length: Option<u64>, cap: u64) -> bool {
    content_length.is_none_or(|len| len <= cap)
}

impl NativeResolver for DebuginfodResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        // Reject an attacker-controlled `build_id` up front: never cache or
        // fetch anything for a non-hex / path-traversal value.
        if !is_valid_build_id(&request.build_id) {
            return None;
        }
        self.resolver_for_build_id(&request.build_id)
            .and_then(|resolver| resolver.symbolize(request))
    }
}

impl NativeResolver for ObjectSymbolResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        // The bytes may be an untrusted, crafted ELF/DWARF blob. Contain any
        // parser panic so a single malicious artifact cannot crash the worker.
        let bytes = Arc::clone(&self.bytes);
        let path = self.path.clone();
        let filename = request.filename.clone();
        let address = request.address;
        std::panic::catch_unwind(move || {
            let object = object::File::parse(bytes.as_slice()).ok()?;
            let frames = path
                .as_ref()
                .and_then(|path| loader_frames(path, address))
                .or_else(|| loader_frames_from_bytes(&bytes, address));
            if let Some(frames) = frames
                && !frames.is_empty()
            {
                return Some(frames);
            }
            let function = nearest_symbol_name(&object, address)
                .unwrap_or_else(|| format!("{filename}+0x{address:x}"));
            Some(vec![NativeSymbol {
                function,
                file: filename,
                line: 0,
            }])
        })
        .unwrap_or(None)
    }
}

fn loader_frames(path: &std::path::Path, address: u64) -> Option<Vec<NativeSymbol>> {
    let loader = addr2line::Loader::new(path).ok()?;
    let mut frames = loader.find_frames(address).ok()?;
    let mut out = Vec::new();
    while let Some(frame) = frames.next().ok()? {
        let location = frame.location;
        let function = frame
            .function
            .and_then(|function| function.demangle().ok().map(std::borrow::Cow::into_owned))
            .or_else(|| loader.find_symbol(address).map(ToString::to_string))
            .unwrap_or_default();
        let file = location
            .as_ref()
            .and_then(|location| location.file)
            .unwrap_or_default()
            .to_string();
        let line = location
            .and_then(|location| location.line)
            .and_then(|line| i32::try_from(line).ok())
            .unwrap_or_default();
        if !function.is_empty() || !file.is_empty() || line != 0 {
            out.push(NativeSymbol {
                function,
                file,
                line,
            });
        }
    }
    Some(out)
}

fn loader_frames_from_bytes(bytes: &[u8], address: u64) -> Option<Vec<NativeSymbol>> {
    // `addr2line::Loader` requires a filesystem path. Use a `NamedTempFile`
    // (O_EXCL, 0600, auto-removed on drop) instead of a predictable temp path
    // so the untrusted blob cannot be targeted by a symlink/TOCTOU attack.
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().ok()?;
    file.write_all(bytes).ok()?;
    file.flush().ok()?;
    loader_frames(file.path(), address)
}

fn nearest_symbol_name(object: &object::File<'_>, address: u64) -> Option<String> {
    object
        .symbols()
        .filter(|symbol| symbol.address() <= address)
        .filter(|symbol| {
            let size = symbol.size();
            size == 0 || address < symbol.address().saturating_add(size)
        })
        .max_by_key(object::ObjectSymbol::address)
        .and_then(|symbol| symbol.name().ok())
        .map(ToString::to_string)
}

pub struct LazySymbolizer<R: NativeResolver> {
    symbols: SymbolDb,
    resolver: Arc<R>,
    cache: Mutex<HashMap<SymbolizeRequest, Option<Vec<Frame>>>>,
}

impl<R: NativeResolver> LazySymbolizer<R> {
    #[must_use]
    pub fn new(symbols: SymbolDb, resolver: Arc<R>) -> Self {
        Self {
            symbols,
            resolver,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn symbolize_location(&self, location: RawLocation) -> Vec<Frame> {
        if location.mapping.symbolization.has_functions() {
            return Vec::new();
        }
        let request = SymbolizeRequest {
            build_id: location.build_id,
            filename: location.filename,
            address: location
                .address
                .saturating_sub(location.mapping.memory_start)
                + location.mapping.file_offset,
        };
        if let Some(cached) = lock_recover(&self.cache).get(&request) {
            return cached.clone().unwrap_or_default();
        }
        let resolved = self.resolver.symbolize(&request).map(|symbols| {
            symbols
                .into_iter()
                .map(|symbol| Frame {
                    function: symbol.function,
                    file: symbol.file,
                    line: symbol.line,
                })
                .collect::<Vec<_>>()
        });
        lock_recover(&self.cache).insert(request, resolved.clone());
        resolved.unwrap_or_default()
    }
}

impl<R: NativeResolver> SymbolSource for LazySymbolizer<R> {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        let frames = self.symbols.resolve(partition, id);
        if !frames.is_empty() {
            return frames;
        }
        self.symbols
            .raw_locations(partition, id)
            .into_iter()
            .flat_map(|location| self.symbolize_location(location))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::{assert, check};
    // Only used by the ELF/DWARF self-symbolization tests below, which run on Linux.
    #[cfg(target_os = "linux")]
    use object::{Object, ObjectSymbol};

    use super::*;
    use crate::{LocationRec, MappingRec, MappingSymbolization};

    struct FixedResolver {
        calls: AtomicUsize,
        expected_address: u64,
    }

    impl NativeResolver for FixedResolver {
        fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert!(request.build_id == "build-a");
            assert!(request.address == self.expected_address);
            Some(vec![NativeSymbol {
                function: "native_main".to_string(),
                file: "main.c".to_string(),
                line: 42,
            }])
        }
    }

    #[cfg(target_os = "linux")]
    fn is_llvm_cov_run() -> bool {
        std::env::var_os("LLVM_PROFILE_FILE").is_some()
    }

    #[test]
    fn lazy_symbolizer_resolves_unsymbolized_location_once() {
        let mut db = SymbolDb::new();
        let filename = db.intern_string("/bin/app");
        let build_id = db.intern_string("build-a");
        let mapping = db.intern_mapping(MappingRec {
            memory_start: 0x1000,
            memory_limit: 0x2000,
            file_offset: 0x30,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        let loc = db.intern_location(LocationRec {
            address: 0x1010,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let stack = db.intern_stacktrace(0, &[loc]);
        let resolver = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0x40,
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let first = source.resolve(0, stack);
        let second = source.resolve(0, stack);

        check!(first == second);
        check!(first[0].function == "native_main");
        check!(resolver.calls.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn lazy_symbolizer_keeps_presymbolized_frames() {
        let mut db = SymbolDb::new();
        let name = db.intern_string("known");
        let function = db.intern_function(crate::FunctionRec {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
        let loc = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![crate::LineRec {
                function_id: function,
                line: 7,
            }],
        });
        let stack = db.intern_stacktrace(0, &[loc]);
        let resolver = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0,
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let frames = source.resolve(0, stack);

        assert!(frames[0].function == "known");
        assert!(resolver.calls.load(Ordering::Relaxed) == 0);
    }

    // Reads DWARF embedded in the test binary itself. Only Linux ships DWARF in
    // the executable; macOS keeps it in a separate .dSYM and Windows in a PDB.
    #[cfg(target_os = "linux")]
    #[test]
    fn object_symbol_resolver_reads_dwarf_from_local_elf() {
        // cargo-llvm-cov instruments the test binary and can make
        // self-symbolization resolve the anchor address to a nearby frame.
        if is_llvm_cov_run() {
            return;
        }
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let address = {
            let object = object::File::parse(bytes.as_slice()).unwrap();
            object
                .symbols()
                .find(|symbol| {
                    symbol.address() != 0
                        && symbol
                            .name()
                            .is_ok_and(|name| name.contains("object_symbol_anchor"))
                })
                .unwrap()
                .address()
        };
        let resolver = ObjectSymbolResolver::from_bytes(bytes).unwrap();

        let frames = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: std::env::current_exe()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                address,
            })
            .unwrap();

        assert!(
            frames
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn byte_backed_object_symbol_resolver_reads_dwarf_locations() {
        // Skip under llvm-cov coverage instrumentation. This test self-symbolizes
        // the test binary and asserts the anchor resolves to its exact file+line.
        // Coverage instrumentation rewrites the binary's code and line tables, so
        // on some toolchains addr2line resolves the anchor's address to a frame
        // this assertion rejects (observed only on CI's `cargo llvm-cov nextest`
        // runner — it passes in every non-coverage Linux build, including a
        // faithful local llvm-cov+nextest repro). `LLVM_PROFILE_FILE` is set by
        // cargo-llvm-cov for the instrumented test process, so use it to detect a
        // coverage run; the test still runs in normal dev/CI builds.
        if is_llvm_cov_run() {
            return;
        }
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let address = {
            let object = object::File::parse(bytes.as_slice()).unwrap();
            object
                .symbols()
                .find(|symbol| {
                    symbol.address() != 0
                        && symbol
                            .name()
                            .is_ok_and(|name| name.contains("object_symbol_anchor"))
                })
                .unwrap()
                .address()
        };
        let resolver = ObjectSymbolResolver::from_bytes(bytes).unwrap();

        let frames = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();

        assert!(
            frames
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
        let exe_path = std::env::current_exe().unwrap();
        let exe_has_file_line_dwarf = loader_frames(&exe_path, address)
            .is_some_and(|frames| frames.iter().any(is_object_symbol_anchor_location));
        if !exe_has_file_line_dwarf {
            return;
        }

        assert!(frames.iter().any(is_object_symbol_anchor_location));
    }

    #[cfg(target_os = "linux")]
    fn is_object_symbol_anchor_location(frame: &NativeSymbol) -> bool {
        frame.function.contains("object_symbol_anchor")
            && frame.file.ends_with("symbolizer.rs")
            && frame.line > 0
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn debuginfod_resolver_fetches_and_caches_build_id_artifact() {
        if is_llvm_cov_run() {
            return;
        }
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let address = {
            let object = object::File::parse(bytes.as_slice()).unwrap();
            object
                .symbols()
                .find(|symbol| {
                    symbol.address() != 0
                        && symbol
                            .name()
                            .is_ok_and(|name| name.contains("object_symbol_anchor"))
                })
                .unwrap()
                .address()
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let max_debuginfo = ByteSize::from_bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let served = Arc::new(AtomicUsize::new(0));
        let served_clone = Arc::clone(&served);
        let server_thread = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let (mut stream, _) = accept_with_deadline(&listener);
            let mut request = [0_u8; 1024];
            let read = std::io::Read::read(&mut stream, &mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /buildid/deadbeef/debuginfo "));
            served_clone.fetch_add(1, Ordering::Relaxed);
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                bytes.len()
            );
            std::io::Write::write_all(&mut stream, header.as_bytes()).unwrap();
            std::io::Write::write_all(&mut stream, &bytes).unwrap();
        });
        let resolver =
            DebuginfodResolver::with_max_debuginfo(vec![base_url], max_debuginfo).unwrap();

        let first = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "deadbeef".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();
        let second = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "deadbeef".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();

        server_thread.join().unwrap();
        check!(served.load(Ordering::Relaxed) == 1);
        check!(first == second);
        check!(
            first
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
    }

    #[test]
    fn chained_resolver_falls_through_to_later_resolvers() {
        struct EmptyResolver;

        impl NativeResolver for EmptyResolver {
            fn symbolize(&self, _request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
                None
            }
        }

        let fixed = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0x10,
        });
        let chain = ChainedResolver::new(vec![Arc::new(EmptyResolver), fixed.clone()]);

        let out = chain
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/bin/app".to_string(),
                address: 0x10,
            })
            .unwrap();

        assert!(out[0].function == "native_main");
        assert!(fixed.calls.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn object_symbol_resolver_rejects_invalid_object_bytes() {
        let bytes = b"not an object file".to_vec();

        assert!(parse_object_guarded(&bytes).is_err());
        assert!(ObjectSymbolResolver::from_bytes(bytes).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn file_system_resolver_reads_symbols_from_cached_file() {
        if is_llvm_cov_run() {
            return;
        }
        let _ = object_symbol_anchor();
        let exe = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&exe).unwrap();
        let address = object_symbol_anchor_address(&bytes);
        let resolver = FileSystemResolver::default();
        let request = SymbolizeRequest {
            build_id: String::new(),
            filename: exe.to_string_lossy().into_owned(),
            address,
        };

        let first = resolver.symbolize(&request).unwrap();
        let second = resolver.symbolize(&request).unwrap();

        assert!(
            first
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
        assert!(first == second);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn nearest_symbol_name_handles_zero_size_and_end_boundaries() {
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let object = object::File::parse(bytes.as_slice()).unwrap();
        let symbols = object
            .symbols()
            .filter_map(|symbol| {
                let name = symbol.name().ok()?;
                (!name.is_empty()).then(|| (symbol.address(), symbol.size(), name.to_string()))
            })
            .collect::<Vec<_>>();
        let (zero_addr, zero_names) = symbols
            .iter()
            .filter(|(address, size, _)| *address != 0 && *size == 0)
            .find_map(|(address, _, _)| {
                let covered_by_sized = symbols.iter().any(|(candidate, size, _)| {
                    *size != 0
                        && *candidate <= *address
                        && *address < (*candidate).saturating_add(*size)
                });
                if covered_by_sized {
                    return None;
                }
                let names = symbols
                    .iter()
                    .filter(|(candidate, size, _)| candidate == address && *size == 0)
                    .map(|(_, _, name)| name.clone())
                    .collect::<Vec<_>>();
                (!names.is_empty()).then_some((*address, names))
            })
            .expect("test binary has an uncovered zero-size symbol");
        assert!(
            nearest_symbol_name(&object, zero_addr)
                .is_some_and(|name| zero_names.iter().any(|candidate| candidate == &name))
        );

        let anchor = object
            .symbols()
            .find(|symbol| {
                symbol.address() != 0
                    && symbol.size() > 0
                    && symbol
                        .name()
                        .is_ok_and(|name| name.contains("object_symbol_anchor"))
            })
            .expect("anchor has a sized symbol");
        let at_end = nearest_symbol_name(&object, anchor.address() + anchor.size());
        assert!(!at_end.is_some_and(|name| name.contains("object_symbol_anchor")));
    }

    #[test]
    fn build_id_validation_accepts_lowercase_hex() {
        for build_id in [
            "deadbeef",
            "0123456789abcdef",
            // Real debuginfod build-ids are 40-char SHA-1 hex digests.
            "aabbccddeeff00112233445566778899aabbccdd",
            // Minimum length is two hex digits.
            "ab",
        ] {
            assert!(is_valid_build_id(build_id), "{build_id}");
        }
    }

    #[test]
    fn build_id_validation_rejects_traversal_and_non_hex() {
        for build_id in [
            // Path traversal and slashes must never reach URL construction.
            "../x",
            "a/b",
            "..",
            "foo/../bar",
            // Uppercase is not a valid lowercase-hex build-id.
            "DEADBEEF",
            "AbCd",
            // Empty / single char / non-hex bytes.
            "",
            "a",
            "xyz",
            "dead beef",
            "build-a",
        ] {
            assert!(!is_valid_build_id(build_id), "{build_id:?}");
        }
    }

    #[test]
    fn debuginfod_rejects_invalid_build_id_without_fetching() {
        // Point at an address that would refuse connections; an invalid
        // build_id must short-circuit before any network attempt is made.
        let resolver = DebuginfodResolver::new(vec!["http://127.0.0.1:1".to_string()]).unwrap();

        let out = resolver.symbolize(&SymbolizeRequest {
            build_id: "../etc/passwd".to_string(),
            filename: "/bin/app".to_string(),
            address: 0x10,
        });

        assert!(out.is_none());
    }

    #[test]
    fn debuginfod_build_url_pushes_segments_safely() {
        let base = reqwest::Url::parse("https://debuginfod.example/").unwrap();
        let url = DebuginfodResolver::build_url(&base, "deadbeef").unwrap();

        assert!(url.as_str() == "https://debuginfod.example/buildid/deadbeef/debuginfo");
        // Host is untouched and there is exactly one path beyond the prefix.
        assert!(url.host_str() == Some("debuginfod.example"));
    }

    #[test]
    fn debuginfod_build_url_keeps_existing_base_path() {
        let base = reqwest::Url::parse("https://proxy.example/debuginfod").unwrap();
        let url = DebuginfodResolver::build_url(&base, "abcd").unwrap();

        assert!(url.as_str() == "https://proxy.example/debuginfod/buildid/abcd/debuginfo");
    }

    #[test]
    fn read_capped_rejects_oversized_body() {
        // The size-cap logic must abort once the accumulated bytes exceed the
        // ceiling. Drive it through an in-memory reader to keep the test
        // cross-platform (no socket / Linux ELF needed).
        let cap: u64 = 1024;
        let cap_usize = usize::try_from(cap).unwrap();
        let oversized = vec![0_u8; cap_usize + 1];
        let out = read_capped_reader(&oversized[..], cap);
        assert!(out.is_none());

        let exact = vec![7_u8; cap_usize];
        let out = read_capped_reader(&exact[..], cap).unwrap();
        assert!(out.len() == cap_usize);
    }

    #[test]
    fn content_length_cap_allows_absent_and_exact_lengths_only() {
        for (content_length, want) in [(None, true), (Some(10), true), (Some(11), false)] {
            assert!(
                content_length_within_cap(content_length, 10) == want,
                "{content_length:?}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn debuginfod_does_not_follow_redirects() {
        // A debuginfod server that 302-redirects must NOT be followed: this is
        // a core SSRF-pivot defence. Serve a redirect to an alternate path on
        // the same listener and assert the resolver gives up (returns None)
        // rather than chasing the Location header.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let followed = Arc::new(AtomicUsize::new(0));
        let followed_clone = Arc::clone(&followed);
        let server_thread = std::thread::spawn(move || {
            // First (and only expected) request: reply with a redirect.
            listener
                .set_nonblocking(true)
                .expect("set listener non-blocking");
            let (mut stream, _) = accept_with_deadline(&listener);
            let mut request = [0_u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut request).unwrap();
            let response = "HTTP/1.1 302 Found\r\nlocation: /elsewhere\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            std::io::Write::write_all(&mut stream, response.as_bytes()).unwrap();
            // If the client wrongly follows the redirect, a second connection
            // arrives; record it.
            std::thread::sleep(std::time::Duration::from_millis(200));
            if listener.accept().is_ok() {
                followed_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        let resolver = DebuginfodResolver::new(vec![base_url]).unwrap();
        let out = resolver.symbolize(&SymbolizeRequest {
            build_id: "deadbeef".to_string(),
            filename: "/missing/on/disk".to_string(),
            address: 0x10,
        });

        server_thread.join().unwrap();
        assert!(out.is_none());
        assert!(followed.load(Ordering::Relaxed) == 0);
    }

    #[cfg(target_os = "linux")]
    fn object_symbol_anchor_address(bytes: &[u8]) -> u64 {
        let object = object::File::parse(bytes).unwrap();
        object
            .symbols()
            .find(|symbol| {
                symbol.address() != 0
                    && symbol
                        .name()
                        .is_ok_and(|name| name.contains("object_symbol_anchor"))
            })
            .unwrap()
            .address()
    }

    #[cfg(target_os = "linux")]
    fn accept_with_deadline(
        listener: &std::net::TcpListener,
    ) -> (std::net::TcpStream, std::net::SocketAddr) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match listener.accept() {
                Ok(stream) => return stream,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for debuginfod request"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(err) => panic!("accept failed: {err}"),
            }
        }
    }

    // Anchor symbol the Linux-only DWARF tests locate in the test binary.
    #[cfg(target_os = "linux")]
    #[inline(never)]
    fn object_symbol_anchor() -> u64 {
        42
    }
}

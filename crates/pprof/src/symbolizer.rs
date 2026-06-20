//! Lazy native symbolization wrapper.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{Frame, RawLocation, SymbolDb, SymbolSource};
use object::{Object, ObjectSymbol};

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
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        object::File::parse(bytes.as_slice()).map_err(|err| err.to_string())?;
        Ok(Self {
            bytes: Arc::new(bytes),
            path: None,
        })
    }

    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let bytes = std::fs::read(&path).map_err(|err| err.to_string())?;
        object::File::parse(bytes.as_slice()).map_err(|err| err.to_string())?;
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
        let mut cache = self.cache.lock().expect("filesystem resolver cache mutex");
        let resolver = cache
            .entry(request.filename.clone())
            .or_insert_with(|| ObjectSymbolResolver::from_file(&request.filename).ok());
        resolver
            .as_ref()
            .and_then(|resolver| resolver.symbolize(request))
    }
}

pub struct DebuginfodResolver {
    base_urls: Vec<String>,
    client: reqwest::blocking::Client,
    cache: Mutex<HashMap<String, Option<ObjectSymbolResolver>>>,
}

impl DebuginfodResolver {
    pub fn new(base_urls: Vec<String>) -> Result<Self, String> {
        let base_urls = base_urls
            .into_iter()
            .filter(|url| !url.trim().is_empty())
            .map(|url| url.trim_end_matches('/').to_string())
            .collect::<Vec<_>>();
        if base_urls.is_empty() {
            return Err("at least one debuginfod base URL is required".to_string());
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self {
            base_urls,
            client,
            cache: Mutex::new(HashMap::new()),
        })
    }

    fn resolver_for_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        let mut cache = self.cache.lock().expect("debuginfod cache mutex");
        if let Some(cached) = cache.get(build_id) {
            return cached.clone();
        }
        let resolver = self.fetch_build_id(build_id);
        cache.insert(build_id.to_string(), resolver.clone());
        resolver
    }

    fn fetch_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        for base_url in &self.base_urls {
            let url = format!("{base_url}/buildid/{build_id}/debuginfo");
            let Ok(response) = self.client.get(url).send() else {
                continue;
            };
            if !response.status().is_success() {
                continue;
            }
            let Ok(bytes) = response.bytes() else {
                continue;
            };
            if let Ok(resolver) = ObjectSymbolResolver::from_bytes(bytes.to_vec()) {
                return Some(resolver);
            }
        }
        None
    }
}

impl NativeResolver for DebuginfodResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        if request.build_id.is_empty() {
            return None;
        }
        self.resolver_for_build_id(&request.build_id)
            .and_then(|resolver| resolver.symbolize(request))
    }
}

impl NativeResolver for ObjectSymbolResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        let object = object::File::parse(self.bytes.as_slice()).ok()?;
        let frames = self
            .path
            .as_ref()
            .and_then(|path| loader_frames(path, request.address))
            .or_else(|| loader_frames_from_bytes(&self.bytes, request.address));
        if let Some(frames) = frames
            && !frames.is_empty()
        {
            return Some(frames);
        }
        let function = nearest_symbol_name(&object, request.address)
            .unwrap_or_else(|| format!("{}+0x{:x}", request.filename, request.address));
        Some(vec![NativeSymbol {
            function,
            file: request.filename.clone(),
            line: 0,
        }])
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
            .and_then(|function| function.demangle().ok().map(|name| name.into_owned()))
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
    let path = temp_object_path();
    std::fs::write(&path, bytes).ok()?;
    let frames = loader_frames(&path, address);
    let _ = std::fs::remove_file(path);
    frames
}

fn temp_object_path() -> PathBuf {
    let tid = format!("{:?}", std::thread::current().id());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "crabka-pprof-symbolizer-{}-{tid}-{nanos}.debug",
        std::process::id()
    ))
}

fn nearest_symbol_name(object: &object::File<'_>, address: u64) -> Option<String> {
    object
        .symbols()
        .filter(|symbol| symbol.address() <= address)
        .filter(|symbol| {
            let size = symbol.size();
            size == 0 || address < symbol.address().saturating_add(size)
        })
        .max_by_key(|symbol| symbol.address())
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
        if location.mapping.has_functions {
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
        if let Some(cached) = self.cache.lock().expect("cache mutex").get(&request) {
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
        self.cache
            .lock()
            .expect("cache mutex")
            .insert(request, resolved.clone());
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

    use assert2::assert;
    use object::{Object, ObjectSymbol};

    use super::*;
    use crate::{LocationRec, MappingRec};

    struct FixedResolver {
        calls: AtomicUsize,
    }

    impl NativeResolver for FixedResolver {
        fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert!(request.build_id == "build-a");
            assert!(request.address == 0x10);
            Some(vec![NativeSymbol {
                function: "native_main".to_string(),
                file: "main.c".to_string(),
                line: 42,
            }])
        }
    }

    #[test]
    fn lazy_symbolizer_resolves_unsymbolized_location_once() {
        let mut db = SymbolDb::new();
        let filename = db.intern_string("/bin/app");
        let build_id = db.intern_string("build-a");
        let mapping = db.intern_mapping(MappingRec {
            memory_start: 0x1000,
            memory_limit: 0x2000,
            file_offset: 0,
            filename,
            build_id,
            has_functions: false,
            has_filenames: false,
            has_line_numbers: false,
            has_inline_frames: false,
        });
        let loc = db.intern_location(LocationRec {
            address: 0x1010,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let stack = db.intern_stacktrace(0, &[loc]);
        let resolver = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let first = source.resolve(0, stack);
        let second = source.resolve(0, stack);

        assert!(first == second);
        assert!(first[0].function == "native_main");
        assert!(resolver.calls.load(Ordering::Relaxed) == 1);
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
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let frames = source.resolve(0, stack);

        assert!(frames[0].function == "known");
        assert!(resolver.calls.load(Ordering::Relaxed) == 0);
    }

    #[test]
    fn object_symbol_resolver_reads_dwarf_from_local_elf() {
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

    #[test]
    fn byte_backed_object_symbol_resolver_reads_dwarf_locations() {
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

        assert!(frames.iter().any(|frame| {
            frame.function.contains("object_symbol_anchor")
                && frame.file.ends_with("symbolizer.rs")
                && frame.line > 0
        }));
    }

    #[test]
    fn debuginfod_resolver_fetches_and_caches_build_id_artifact() {
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
        let served = Arc::new(AtomicUsize::new(0));
        let served_clone = Arc::clone(&served);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let read = std::io::Read::read(&mut stream, &mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /buildid/build-a/debuginfo "));
            served_clone.fetch_add(1, Ordering::Relaxed);
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                bytes.len()
            );
            std::io::Write::write_all(&mut stream, header.as_bytes()).unwrap();
            std::io::Write::write_all(&mut stream, &bytes).unwrap();
        });
        let resolver = DebuginfodResolver::new(vec![base_url]).unwrap();

        let first = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();
        let second = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();

        server.join().unwrap();
        assert!(served.load(Ordering::Relaxed) == 1);
        assert!(first == second);
        assert!(
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

    #[inline(never)]
    fn object_symbol_anchor() -> u64 {
        42
    }
}

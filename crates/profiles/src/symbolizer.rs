//! Symbolizer role plumbing.

use std::sync::Arc;

use crabka_pprof::{
    ChainedResolver, DebuginfodResolver, FileSystemResolver, NativeResolver, NativeSymbol,
    SymbolizeRequest,
};

#[derive(Clone, Debug, Default)]
pub struct AddressFallbackResolver;

impl NativeResolver for AddressFallbackResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        Some(vec![NativeSymbol {
            function: format!("{}+0x{:x}", build_label(request), request.address),
            file: request.filename.clone(),
            line: 0,
        }])
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn native_resolver_from_debuginfod_urls(
    urls: Vec<String>,
) -> Result<ChainedResolver, crate::ProfilesError> {
    let mut resolvers: Vec<Arc<dyn NativeResolver>> = vec![Arc::new(FileSystemResolver::default())];
    if !urls.is_empty() {
        let debuginfod = DebuginfodResolver::new(urls).map_err(crate::ProfilesError::Block)?;
        resolvers.push(Arc::new(debuginfod));
    }
    resolvers.push(Arc::new(AddressFallbackResolver));
    Ok(ChainedResolver::new(resolvers))
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run(debuginfod_urls: Vec<String>) -> Result<(), crate::ProfilesError> {
    let _resolver = native_resolver_from_debuginfod_urls(debuginfod_urls.clone())?;
    tracing::info!(
        debuginfod_urls = ?debuginfod_urls,
        "profiles symbolizer ready; DWARF/debuginfod resolver integration is loaded"
    );
    tokio::signal::ctrl_c()
        .await
        .map_err(|err| crate::ProfilesError::Block(format!("symbolizer signal failed: {err}")))?;
    Ok(())
}

fn build_label(request: &SymbolizeRequest) -> String {
    if request.build_id.is_empty() {
        request.filename.clone()
    } else {
        request.build_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn fallback_resolver_names_build_id_and_offset() {
        let out = AddressFallbackResolver
            .symbolize(&SymbolizeRequest {
                build_id: "abc".to_string(),
                filename: "/bin/app".to_string(),
                address: 0x42,
            })
            .unwrap();

        assert!(out[0].function == "abc+0x42");
        assert!(out[0].file == "/bin/app");
    }

    #[test]
    fn symbolizer_builds_local_plus_debuginfod_resolver() {
        native_resolver_from_debuginfod_urls(vec!["http://127.0.0.1:1".to_string()]).unwrap();
    }

    #[test]
    fn native_resolver_falls_back_to_address_frame() {
        let resolver = native_resolver_from_debuginfod_urls(Vec::new()).unwrap();
        let out = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: "/missing/native".to_string(),
                address: 0x99,
            })
            .unwrap();

        assert!(out[0].function == "/missing/native+0x99");
        assert!(out[0].file == "/missing/native");
    }
}

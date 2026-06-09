//! Emit `impl crate::ProtocolRequest for <RequestType>` blocks.
//!
//! Only emitted for `MessageType::Request` schemas. The matching Response
//! type is derived by replacing the `Request` suffix with `Response` in
//! the type name, and converting the result to a `snake_case` module name.

use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Emit the `impl crate::ProtocolRequest for <Type>` block for a request spec.
///
/// Returns `None` if the spec is not a `MessageType::Request`.
#[must_use]
pub fn emit_protocol_request(spec: &MessageSpec) -> Option<String> {
    emit_protocol_request_tokens(spec).map(|tokens| tokens.to_string())
}

pub fn emit_protocol_request_tokens(spec: &MessageSpec) -> Option<TokenStream> {
    if spec.message_type != MessageType::Request {
        return None;
    }

    let type_name = format_ident!("{}", name_conv::type_name(&spec.name));

    // Derive the response type name: replace trailing "Request" with "Response".
    // Every Kafka request has a matching response with exactly this naming convention.
    let type_name_string = type_name.to_string();
    let response_type_name = if let Some(prefix) = type_name_string.strip_suffix("Request") {
        format!("{prefix}Response")
    } else {
        // Fallback: append "Response" directly (should not occur in practice).
        format!("{type_name_string}Response")
    };

    let response_module = format_ident!("{}", name_conv::module_name(&response_type_name));
    let response_type_name = format_ident!("{response_type_name}");

    Some(quote! {
        impl crate::ProtocolRequest for #type_name {
            const API_KEY: i16 = API_KEY;
            const MIN_VERSION: i16 = MIN_VERSION;
            const MAX_VERSION: i16 = MAX_VERSION;
            const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;
            type Response = super::#response_module::#response_type_name;
        }
    })
}

//! Emit `impl crate::ProtocolRequest for <RequestType>` blocks.
//!
//! Only emitted for `MessageType::Request` schemas. The matching Response
//! type is derived by replacing the `Request` suffix with `Response` in
//! the type name, and converting the result to a `snake_case` module name.

use std::fmt::Write;

use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;

/// Emit the `impl crate::ProtocolRequest for <Type>` block for a request spec.
///
/// Returns `None` if the spec is not a `MessageType::Request`.
#[must_use]
pub fn emit_protocol_request(spec: &MessageSpec) -> Option<String> {
    if spec.message_type != MessageType::Request {
        return None;
    }

    let type_name = name_conv::type_name(&spec.name);

    // Derive the response type name: replace trailing "Request" with "Response".
    // Every Kafka request has a matching response with exactly this naming convention.
    let response_type_name = if let Some(prefix) = type_name.strip_suffix("Request") {
        format!("{prefix}Response")
    } else {
        // Fallback: append "Response" directly (should not occur in practice).
        format!("{type_name}Response")
    };

    let response_module = name_conv::module_name(&response_type_name);

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(
        out,
        "impl crate::ProtocolRequest for {type_name} {{"
    )
    .unwrap();
    writeln!(out, "    const API_KEY: i16 = API_KEY;").unwrap();
    writeln!(out, "    const MIN_VERSION: i16 = MIN_VERSION;").unwrap();
    writeln!(out, "    const MAX_VERSION: i16 = MAX_VERSION;").unwrap();
    writeln!(out, "    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;").unwrap();
    writeln!(
        out,
        "    type Response = super::{response_module}::{response_type_name};"
    )
    .unwrap();
    writeln!(out, "}}").unwrap();

    Some(out)
}

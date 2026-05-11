# Known issues

## Differential testing gaps

### Headers (RequestHeader, ResponseHeader)

JVM-differential tests not yet wired. The current oracle exposes only
ApiKey-indexed messages; headers are independent framing types and
require an oracle extension (new `header_encode` / `header_decode` ops
using `RequestHeaderDataJsonConverter` and `ResponseHeaderDataJsonConverter`
directly, without going through `ApiKeys.forId`).

Inline round-trip tests and snapshot tests gate correctness for now.

Tracking: sub-plan 1d will revisit when extending the oracle to
cover all 197 schemas.

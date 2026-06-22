# Loki Logs Wedge — Storage/Query Spike Findings

Date: 2026-06-18
Status: Findings (throwaway spike complete)
Spike crate: `crates/observability-spike`
Production slices promoted from spike: `crates/blockstore`, `crates/logql`,
`crates/observability`

## TL;DR / decision gate

**GO.** The basic Loki wedge storage/query shape is viable:

- Canonical stream labels can be fingerprinted deterministically.
- A simple inverted label index prunes `{label="value"}` selectors to matching
  series before scanning rows.
- A minimal LogQL parser/evaluator can cover exact/negative/regex label
  matchers, line filters (`|=`, `!=`, `|~`, `!~`), `| json` / `| logfmt`
  parser stages, and field filters without committing to the full language yet.
- The production planning chain now exists for stream queries:
  LogQL matchers → tenant-scoped label index → series fingerprints → block
  index → candidate object keys.
- The role-selectable observability binary surface now exists for the
  distributor / compactor / querier process model, including role-specific
  HTTP router construction, object-store construction from config, and listener
  startup for the distributor and querier.
- Distributor runtime startup can now build a Kafka-backed WAL sink from
  `--wal-bootstrap-server` / `--wal-topic`, serialize normalized log records as
  tenant/series-keyed Kafka producer records, and await `acks=all` delivery.
- Kafka WAL values can now be decoded back into positioned `WalLogRecord`
  values from consumed partition/offset metadata, and the compactor has a raw
  Kafka-record adapter that decodes batches before writing blocks or committing
  offsets.
- Native Kafka-produced log records can now enter the same compactor path:
  record values are treated as log lines while headers carry tenant, labels,
  nanosecond timestamps, and structured metadata.
- The compactor path now has a Kafka-consumer-facing service seam:
  `LogWalConsumer` polls raw WAL records, `compact_next_kafka_wal_batch...`
  writes object-store blocks/indexes, and only then commits the consumed
  partition offset.
- The compactor role can now run one config/dependency-driven compaction
  iteration: service config supplies the object-store prefix, dependencies
  supply the WAL consumer, and the runtime rejects missing object store or WAL
  consumer wiring before processing.
- The compactor service dispatch now serves the compactor HTTP router while the
  compactor path runs for the `compactor` target, preserving label/block index
  state across multiple polled batches and keeping the compactor alive after
  idle WAL polls until the service is shut down.
- The compactor runtime paths reload the existing tenant manifest before
  compacting each non-empty WAL batch, so a restarted compactor appends new
  block descriptors without dropping blocks written by an earlier process.
- The compactor runtime now splits a single polled WAL batch across tenant /
  partition boundaries, writes each tenant's block and manifest separately, and
  only then commits the completed partition offsets. A mixed-tenant WAL poll no
  longer poisons compaction for all tenants in the batch.
- Reprocessing an uncommitted WAL offset after a commit failure now rewrites the
  deterministic block key and replaces the matching manifest descriptor instead
  of duplicating it, preserving the compactor's no-loss/no-dup crash window.
- The long-running compactor loop now retries object-store-backed compaction
  failures with bounded backoff before committing offsets. Transient block or
  index write failures keep the service alive, leave the WAL batch uncommitted,
  and allow the next poll to compact the same offsets successfully.
- The long-running compactor loop now also retries transient object-store
  failures while persisting the compaction-frontier manifest after a committed
  batch, so a short frontier-write outage does not tear down the service or
  strand a stale hot/cold boundary.
- The first distributor ingress surface now accepts Loki JSON and
  snappy-compressed protobuf push payloads, normalizing both into
  tenant-scoped WAL records behind a sink trait.
- Loki JSON push payloads with `Content-Encoding: gzip` now decompress before
  normalization, matching common client behavior for compressed HTTP pushes.
- Loki JSON and protobuf push normalization now rejects malformed stream label
  names before appending to the WAL, and protobuf stream label parsing rejects
  duplicate label names instead of silently collapsing them, matching the
  design's distributor validation path for bad labels. JSON stream-label
  duplicate keys follow Loki's decoder behavior and keep the later value before
  appending to the WAL; real-Loki differential coverage now pins that `204`
  response shape. Real-Loki
  differential coverage now pins the JSON malformed-label response shape:
  invalid stream labels return Loki's plain-text `400` parser error rather than
  the generic JSON error envelope.
- Loki JSON and protobuf push normalization now also rejects malformed
  structured metadata names, and both JSON and protobuf normalization reject
  duplicate structured metadata names, before appending to the WAL.
- Loki JSON push normalization now rejects structured metadata values that are
  not strings, including nested objects, instead of stringifying them before the
  WAL boundary. Real-Loki differential coverage now pins the JSON non-string
  structured-metadata response shape: Loki returns a plain-text `400` decoder
  error instead of the generic JSON error envelope.
- Loki JSON/protobuf push and OTLP JSON normalization now reject invalid
  non-negative timestamp forms, including negative values and out-of-range
  protobuf nanoseconds, before appending to the WAL.
- Native Kafka-produced log-line records now reject malformed
  `crabka-log-label-*` and `crabka-log-metadata-*` header names, plus duplicate
  label or metadata header names, and invalid explicit or broker-derived
  timestamps before they can enter hot-tail or compactor decode paths.
- The distributor also accepts OTLP/HTTP JSON and protobuf logs at
  `POST /v1/logs` and Loki's collector-compatible `POST /otlp/v1/logs`, plus
  OTLP/gRPC logs through the generated tonic service, mapping resource and
  scope attributes to stream labels and log-record attributes to structured
  metadata, preserving protobuf `trace_id` and `span_id` fields as structured
  metadata, preserving OTLP severity fields as structured metadata, normalizing
  OTLP attribute names to Loki-compatible label names, and rejecting duplicate
  normalized OTLP attribute names before they silently collapse into labels or
  structured metadata. It discovers a default `service_name` from Loki's
  candidate labels, falling back to `unknown_service` when none are present.
- The production blockstore can now write and read sorted Parquet log blocks
  using DataFusion's Arrow/Parquet crate line, including native Arrow
  `Map<Utf8, Utf8>` structured metadata.
- Planned block descriptors can now be registered as a custom DataFusion table
  provider so SQL scans touch only the selected Parquet files and advertise
  inexact pushdown for fingerprint, timestamp, and line filters. The same
  provider can now scan planned Parquet blocks through `object_store::ObjectStore`,
  with a shared registration helper for object-store-backed query routes. The
  stream-query and metric-query execution paths now also push the planner's
  selected fingerprints and literal line filters into generated DataFusion SQL,
  with golden SQL assertions pinning both scan shapes.
- Planned cold-block stream queries can now merge with an in-memory hot WAL tail
  after a timestamp or partition/offset compaction frontier and serialize Loki
  `streams` JSON without gaps or duplicates.
- A buffered hot-tail source can now ingest polled Kafka WAL batches, decode both
  Crabka JSON envelopes and native Kafka-header log lines, and expose them through
  the same `LogHotTail` query/tail interface.
- Querier service startup can now wire an injected WAL consumer into that
  buffered hot-tail poller, so role-built query routes can see Kafka-sourced hot
  rows without a hand-populated in-memory tail dependency.
- Querier dependency construction now validates WAL bootstrap config and builds a
  Kafka WAL consumer from `--wal-bootstrap-server`, `--wal-topic`, and
  `--wal-group-id`, feeding the same hot-tail poller used by injected tests.
- A live in-process Crabka broker integration test now drives
  `KafkaLogWalSink::connect` and `KafkaLogWalConsumer::connect` through the real
  Kafka protocol, proving distributor-produced WAL records can be consumed and
  decoded into the buffered hot-tail boundary.
- Config-built distributor and querier startup now have live in-process Crabka
  broker coverage too: distributor router construction writes Loki push payloads
  through a configured Kafka WAL sink, and querier router construction tails a
  configured Kafka WAL consumer into HTTP query results.
- A configured live-broker distributor → compactor → querier loop is now covered
  end-to-end: Loki push writes the WAL, the compactor consumes from the live
  broker into a configured file object store, and the querier serves the
  compacted log back through the Loki query API.
- OTLP/HTTP now has the same configured live-broker loop coverage: resource and
  scope attributes enter through `POST /v1/logs`, compact to object-store
  blocks, and query back through the Loki API with the expected stream labels.
- The configured distributor listener now also serves OTLP/gRPC logs on the
  same bound port as the Loki and OTLP/HTTP routes, so the gRPC ingest door is
  available at the role-selectable service boundary rather than only as an
  in-process tonic service.
- The configured live-broker loop now has tenant-isolation coverage over a
  shared WAL topic: tenant A and tenant B can ingest through the same
  distributor/compactor path, and the querier selects the matching tenant
  object-store manifest from each request's `X-Scope-OrgID` without leaking the
  other tenant's row.
- Configured hot/cold merge now has live-broker coverage too: a querier started
  after one compacted push and one uncompacted push returns the compacted block
  row plus the live WAL-tail row exactly once when the query spans the
  compaction frontier.
- Configured compactor restart now has live-broker coverage: a second compactor
  instance using the same WAL consumer group resumes from the committed offset,
  appends to the existing object-store manifest, and the querier returns both
  compacted rows exactly once.
- Loki-compatible HTTP `query` and `query_range` routes now serve cold-block
  stream results and can merge stream queries with the hot WAL tail for the
  current stream-query subset.
- Stream query routes now honor Loki's `limit` and explicit `direction`
  parameters by ordering/capping returned log entries while preserving Loki's
  `streams` JSON shape.
- Querier routes can now enforce configured maximum query time range,
  matching-series, and planned block-byte limits, returning Loki-style
  `bad_data` errors before executing over-wide or over-broad queries.
- Querier service startup can now attach a configured `LogHotTail` dependency
  plus compaction frontier to the role-built Loki router, so service-created
  query routes can see hot WAL rows as well as cold object-store blocks.
- Compactor and querier paths can now share a live compaction frontier handle:
  committed compactor batches advance per-partition offsets, and query/tail
  paths snapshot that handle when filtering hot WAL rows. The frontier is now
  persisted as an object-store manifest and reloaded across compactor restarts
  and configured querier hot-tail startup.
- Loki-compatible HTTP `tail` now upgrades to a websocket and emits
  Loki-shaped stream frames as new matching records appear in the configured
  hot WAL tail source, filtering hot records through the same compaction
  frontier. Real-Loki differential coverage now pins malformed and missing
  tail query websocket handshake errors before upgrade.
- Configured listener-level tailing now has live-broker coverage too: a
  websocket connected to a configured querier listener receives a Loki-shaped
  tail frame after a configured distributor writes the matching log through the
  live Kafka WAL.
- Loki metadata endpoints now also merge uncompacted hot WAL-tail labels after
  the compaction frontier, so `labels`, `label/{name}/values`, and `series`
  expose recently ingested streams before the compactor writes them into the
  cold index.
- A configured object-store querier can now start without a fixed `--tenant`
  for tenant manifest or shard-catalog indexes and load the object-store index
  selected by each request's `X-Scope-OrgID` header for stream queries and
  metadata endpoints.
- Loki-compatible HTTP error responses now use the Prometheus/Loki-style JSON
  error envelope with `status`, `errorType`, `error`, and `data: null` for the
  query, metadata, and distributor routes covered so far.
- The first metric-shaped LogQL slice now parses `count_over_time(...)`,
  `present_over_time(...)`, `rate(...)`, `bytes_rate(...)`, and `bytes_over_time(...)`, and serves Loki
  `matrix` results from cold blocks plus Loki `vector` results for instant metric
  queries, including stepped `query_range` samples, hot-tail merging for the first
  `count_over_time` path, and vector aggregations with `sum` / `count` / `min` /
  `max` / `avg` plus prefix or trailing `by` / `without` grouping. LogQL metric
  range selectors now accept ordered Prometheus-style compound durations.
  Supported unwrapped range aggregations now also accept direct trailing
  `by(...)` / `without(...)` grouping.
- LogQL label filters now evaluate original stream labels, structured metadata,
  and parser-extracted labels; string filters support Loki's `=~` and `!~`
  regex operators alongside the existing equality and numeric comparison
  filters.
- The default LogQL `| json` parser now flattens nested objects with `_`,
  sanitizes extracted label names, skips arrays, and applies those fields at the
  cold-block query boundary.
- Parser-extracted labels that collide with existing stream labels or metadata
  now use Loki's `_extracted` suffix while preserving the original label value.
- The same planned stream query can now execute directly against
  object-store-backed Parquet blocks, closing the first loop between compactor
  output and querier input.
- The object-store stream executor now preserves readable block results when a
  planned block cannot be read and returns Loki-style `warnings`, starting the
  querier partial-result behavior from the design's block-read failure path.
- Configured querier HTTP routes now retain the configured object store and use
  it for cold stream block reads, so route-level queries can return readable
  object-store rows plus Loki warnings when a planned cold block is missing.
- The object-store stream and metric executors now route planned cold blocks through the
  same `LogBlockTableProvider` / DataFusion table-registration bridge as local
  cold-block queries while preserving per-block Loki warnings for unreadable
  object-store blocks.
- The object-store querier bridge now also serves the current metric-query
  subset from object-store blocks through that provider path, including stepped
  Loki `matrix` responses and partial-result warnings for unreadable planned
  blocks.
- Distributor, querier, and compactor HTTP routers now expose Loki-compatible status
  endpoints at `/ready`, `/log_level`, `/metrics`, `/config`, `/services`, and
  `/loki/api/v1/status/buildinfo`, so health checks, Prometheus scrapes, and
  Grafana/Loki operational probes can get the expected component signals.
  Real-Loki differential coverage now pins the `/ready`, GET `/log_level`,
  `/services`, stable top-level `/config` lines, selected `/metrics` Prometheus
  families, and build-info response shapes while normalizing
  implementation-specific string values where needed.
  `/log_level` now accepts Loki's documented POST query parameter and
  form-encoded body variants for `debug`, `info`, `warn`, and `error`, returning
  Loki's `status` / `message` success envelope, and invalid or missing levels
  return Loki's `failed` / `message` error envelope.
  The distributor router also exposes `/distributor/ring` as a Loki-compatible
  HTML status page for the current single-process distributor. The compactor
  router exposes `/compactor/ring` as the matching Loki-compatible HTML status
  page, and compactor service startup now serves that HTTP surface while the
  long-running compaction loop polls WAL batches. Real-Loki differential
  coverage now pins the stable ring page shape: distributor and compactor ring
  pages expose the `Ring Status` heading with an `ACTIVE` member state, while
  `/ruler/ring` exposes Loki's default `Cortex Ruler Status` page.
- The compactor router now also exposes Loki's delete-request lifecycle API at
  `/loki/api/v1/delete`: `POST` / `PUT` validate tenant, LogQL, and Unix-second
  time bounds before recording an in-process delete request; `GET` lists active
  requests for the tenant; and `DELETE` cancels a request by `request_id`. A
  shared delete request registry can now be attached to compactor and querier
  roles so stream, metric, tail, pattern, and detected-field queries suppress log
  rows whose tenant, timestamp range, and delete LogQL match an active request
  before returning streams, calculating metric samples, streaming live tail
  frames, or aggregating discovery output. Configured local compactor and
  querier roles also persist and reload the active delete registry from
  `data_root`. The compactor now also materializes active delete requests while
  compacting new WAL batches into blocks, so matching rows are omitted from newly
  written block objects while committed offsets still advance through the full
  batch. The compactor maintenance path also rewrites overlapping existing
  local manifests, object-store tenant manifests, and object-store shard-catalog
  blocks for active delete requests and leaves fully deleted blocks out of the
  rewritten manifests.
  Real-Loki differential coverage now records that the default Loki 3.4.2
  container does not mount `/loki/api/v1/delete` and returns `404 page not
  found`; Crabka intentionally serves the delete lifecycle API because the
  compactor/querier delete registry is part of this Loki wedge.
- The querier router now exposes read-only ruler inventory endpoints:
  `/loki/api/v1/rules`, `/prometheus/api/v1/rules`, and
  `/prometheus/api/v1/alerts`. Real-Loki differential coverage now pins the
  default no-rule-dir behavior: Loki's native rules endpoint returns the
  tenant-specific missing-rule-directory `400` text response, while the
  Prometheus rules and alerts endpoints return empty success envelopes with
  `errorType` and `error` fields. The deprecated `/api/prom/rules` root alias
  follows the native Loki rules response, while `/api/prom/alerts` remains
  unmounted and returns Loki's plain-text `404 page not found` body. It also
  exposes `/ruler/ring` as a
  Loki-compatible HTML status page for that current in-process ruler API
  surface.
- The HTTP routers now also expose Loki's deprecated `/api/prom/*` aliases for
  push, query, query_range, tail, label names, label values, and series,
  preserving compatibility with older Loki clients while routing to the same
  handlers. Query, query_range, and series aliases accept the same
  form-encoded POST bodies as their `/loki/api/v1/*` counterparts. Real-Loki
  differential coverage now pins the alias metadata response shapes observed in
  Loki 3.4.2: `/api/prom/label` returns a legacy `values` object, while
  `/api/prom/series` keeps the canonical `status` / `data` envelope. Crabka's
  `/api/prom/label/{name}/values` keeps the useful deprecated label-value
  semantics even though the default Loki 3.4.2 container currently returns the
  same label-name payload as `/api/prom/label` for that path. The deprecated
  instant query alias now also matches Loki's legacy limitation: stream results
  are served, but metric/vector results return the plain-text `400` response
  `legacy endpoints only support streams result type`.
- Loki label metadata endpoints now also accept documented form-encoded POST
  bodies on `/loki/api/v1/labels` and `/loki/api/v1/label/{name}/values`, plus
  the deprecated `/api/prom/label` aliases, sharing the same selector/time
  parameter parser as the GET paths.
- The querier router now also exposes `/loki/api/v1/index/stats`, returning
  Loki-shaped stream/chunk/entry/byte counts from planned local or object-store
  cold blocks for a selector and time range. `/loki/api/v1/query`,
  `/loki/api/v1/query_range`, `/loki/api/v1/series`, and
  `/loki/api/v1/index/stats` accept Loki's documented form-encoded POST body as
  well as URL query parameters. Real-Loki differential coverage now pins the
  `index/stats` success response contract while normalizing numeric counters
  that vary by implementation.
- Loki-compatible `query` and `query_range` HTTP responses now include the
  documented `data.stats` object at the route boundary while keeping the lower
  level stream and metric executor return shapes focused on query results. Stream
  responses fill conservative planned cold-block byte/chunk counters plus
  returned line totals, and metric responses fill planned cold-block byte/chunk
  counters plus returned metric-sample totals. Stream query responses now split
  returned line totals between Loki's `store` stats bucket for planned cold-block
  reads and the `ingester` bucket for matching hot-tail records after
  direction/limit/interval options have been applied. Stream
  queries also follow Loki's default `direction=backward` behavior before
  applying `limit`, so omitted direction returns the newest matching entries;
  real-Loki differential coverage now pins that default ordering and limit
  behavior at the `query_range` route.
- Loki timestamp query params now accept integer Unix nanoseconds, fractional
  Unix seconds, and RFC3339/RFC3339Nano strings on `time`, `start`, and `end`.
- Loki `query_range` now defaults missing bounds to the documented recent range:
  `end=now` and `start=end-1h` unless `since` supplies a different window.
- Loki `since` query params now derive `start` from `end` when `start` is
  absent on query and metadata routes.
- Loki `step` query params now accept the existing integer nanosecond form plus
  documented float-second and Prometheus-duration forms.
- Loki `query` and `query_range` now accept pipe-separated `X-Scope-OrgID`
  tenant headers such as `tenant-a|tenant-b`, fan out the same LogQL request per
  tenant, merge result arrays, and add per-tenant scan stats into one Loki
  response.
- Loki stream `query_range` responses now honor the documented `interval`
  parameter before applying direction and limit.
- Loki metric vector and matrix responses now serialize sample timestamps as
  JSON numbers in Unix seconds, while stream log responses keep Loki's string
  nanosecond timestamps.
- Loki push ingestion now mirrors Loki's default stream enrichment for Grafana
  discovery paths: missing `service_name` labels are derived from the standard
  candidate-label list, and missing log-level labels get a detected
  `detected_level` value when the log line carries a recognizable level token.
- The integration suite now includes real-Loki differential coverage for Loki
  push plus `query_range` stream and metric queries and metadata discovery
  endpoints: it starts a `grafana/loki` container, ingests the same payload into
  Loki and Crabka's distributor/compactor/querier loop, and compares the stable
  `resultType` / `result` response payload plus `labels`, label values, and
  `series` payloads. Metric range coverage now includes both
  `count_over_time(...)` and `rate(...)`. These cases caught and fixed the
  `count_over_time` range-selector lower-bound edge so Crabka now follows
  Loki's `(start, end]` sample window, and the Loki metadata-visibility edge
  where derived `detected_level` appears in stream query results but is not
  advertised by metadata discovery endpoints.
- The real-Loki differential corpus now covers `| json` and `| logfmt` parser
  stages followed by numeric/string field filters, plus push-time structured
  metadata filters. That caught and fixed stream-result fidelity gaps:
  parser-extracted fields now appear in returned stream label sets, with
  collision suffixes such as `_extracted`, and matched structured metadata
  fields such as `trace_id` and `status` now appear in returned stream label
  sets.
- LogQL field filters now also parse and evaluate Loki's duration and byte
  comparison literals, such as `duration >= 20ms` and
  `bytes_consumed > 20MB`; the parser differential corpus includes a real-Loki
  comparison for the `logfmt` pipeline form.
- LogQL field filters also support `and` / `or` chains within a single
  label-filter stage, plus Loki's comma and adjacent-predicate forms as
  equivalent `and` separators, with real-Loki differential coverage for the
  `logfmt` pipeline forms.
- Real-Loki differential coverage now pins the in-scope selector and line
  filter operators together: regex and negative label matchers (`=~`, `!=`)
  combined with contains, not-contains, regex, and not-regex line filters
  (`|=`, `!=`, `|~`, `!~`) return the same stream rows as Loki.
- LogQL line filters now also parse and evaluate Loki's pattern filter
  operators (`|>` and `!>`) for documented `<_>` wildcard patterns, and string
  literals now accept Loki's raw backtick form as well as escaped double quotes.
- LogQL pipelines now include Loki's `| decolorize` stage for stripping ANSI
  color sequences from the current line before later filters, parsers, and
  returned stream values.
- LogQL field filters now also accept Loki's raw backtick string form for
  string and regex comparisons, matching the documented double-quote/backtick
  alternatives.
- LogQL parser stages now include Loki's `| pattern` parser for documented
  `<name>` captures and `<_>` skips, plus Loki's `| regexp` parser for named
  `(?P<name>...)` captures, and Loki's `| unpack` parser for packed JSON lines
  with `_entry` line replacement. Captured and unpacked values participate in
  later field filters and returned stream labels, and non-matching lines expose
  parser-specific `__error__` values for downstream error filters.
- LogQL `| logfmt` now supports Loki's parameterized extraction form such as
  `| logfmt host, fwd_ip="fwd"`, extracting only requested fields and renaming
  source keys into returned labels. The parser also accepts Loki's
  `--keep-empty` and `--strict` flags, retaining standalone keys as empty
  labels and surfacing malformed strict-mode tokens through `LogfmtParserErr`;
  real-Loki differential coverage pins the flag behavior, Loki's strict-mode
  standalone-key handling, strict error details, and string field filters that
  compare missing extracted labels as empty strings. Parameterized logfmt now
  also keeps missing requested fields as empty labels, and numeric comparisons
  against those empty values retain the row with Loki's `LabelFilterErr`
  metadata. Default logfmt extraction now also sanitizes invalid extracted
  field-name characters before returning stream labels, including ANSI-prefixed
  keys such as `_31mstatus`.
- LogQL `| json` now supports Loki's selected extraction form such as
  `| json method="request.method", first="servers[0]"`, including dotted field
  access, bracketed field names, and array indexes.
- LogQL formatting stages now include the core Loki `| line_format` expression
  for `{{.label}}` interpolation and `{{__line__}}` / `{{line}}` current-line
  access; the formatted line participates in later line filters and returned
  stream values. Timestamp-aware stream execution also exposes Loki's
  `{{__timestamp__}}` / `{{timestamp}}` template value, with `unixEpoch`,
  `unixEpochMillis`, and `unixEpochNanos`
  helpers plus `date` layout formatting for rendering the current log row
  timestamp. String epoch fields can also be converted with `unixToTime`
  before date or epoch rendering, covering Loki's documented day, second,
  millisecond, microsecond, and nanosecond epoch forms. The current server time
  is available through Loki's `now` template helper for date and epoch
  pipelines.
  The template evaluator also supports Loki-style string pipelines for the
  compact function subset `lower`, `upper`, `replace`, `default`, `substr`,
  `title`, `trim`, `trimAll`, `trimPrefix`, `trimSuffix`, `trunc`,
  `urlencode`, `urldecode`, `urlquery`, `html`, `js`, `b64enc`, `b64dec`, `date`, `bytes`, `duration`,
  `duration_seconds`, `print`, `printf`, `println`, `len`, `unixToTime`, `toDate`, `toDateInZone`, `now`,
  `fromJson` with `index` / `slice` helpers and `range` blocks over decoded arrays, integer math helpers (`add`,
  `sub`, `mul`, `div`, `mod`, `max`, `min`, `int`), float math helpers
  (`addf`, `subf`, `mulf`, `divf`, `maxf`, `minf`, `float64`, `ceil`,
  `floor`, `round`), `contains`, `eq`, `ne`, `lt`, `le`, `gt`, `ge`,
  `hasPrefix`, `hasSuffix`, `alignLeft`, `alignRight`, `indent`, `nindent`, `repeat`,
  `count`, `regexReplaceAll`, and `regexReplaceAllLiteral`, plus `if` / `else if` / `else` / `end`
  conditional blocks over those helper expressions, including parenthesized
  `and` / `or` / `not` boolean combinators, and Go-template variable
  declarations such as `{{ $line := __line__ }}` for reuse in later actions.
- LogQL label formatting now includes Loki's `| label_format` rename form
  (`dst=src`) and template form (`dst="{{.src}}"`), with rewritten labels
  available to later filters and returned stream label sets.
- LogQL line-format templates now support Loki/Go-template `with` control
  blocks, including `else` branches and `{{ . }}` rendering of the selected
  value inside the block.
- LogQL line-format templates now also honor Loki/Go-template whitespace trim
  markers on actions, so `{{- ... -}}` trims surrounding literal whitespace
  before subsequent line filters see the formatted line.
- LogQL line-format templates now ignore Loki/Go-template comment actions such
  as `{{/* hidden */}}` while preserving the surrounding rendered text.
- LogQL line-format templates now accept Loki/Go-template variable declarations
  such as `{{ $method := .method }}`, keeping those variables available to later
  sibling actions in the same rendered template.
- LogQL label expressions now include Loki's `| drop` and `| keep` stages for
  bare label names, exact value matchers, and regex value matchers; resulting
  label sets participate in later filters and returned stream labels.
- LogQL parsing now ignores Loki-style `#` comments outside string literals,
  including whole-line and trailing comments in multi-line stream queries;
  formatter output normalizes those queries without comments.
- Empty-compatible regex equality matchers now follow Loki/Prometheus absent
  label semantics in both direct LogQL evaluation and label-index planning:
  with another non-empty selector such as `app="api"`, `env=~".*"` also matches
  streams that do not carry an `env` label.
- LogQL stream and metric selectors now reject Loki/Prometheus-invalid matcher
  sets that can match the empty label set, such as `{}`, `{env=~".*"}`, or
  `{env!="prod"}`, while still allowing those empty-compatible matchers when
  paired with a non-empty-compatible matcher.
- Regex label matchers are anchored in both direct LogQL evaluation and
  label-index planning, so `{app=~"api|worker"}` matches `api` and `worker`
  but not `myapi` or `api-v2`. Line and pipeline field regex filters keep their
  existing unanchored filter behavior.
- Real-Loki differential coverage now also pins pipeline comparison filters
  over original stream labels. A selector such as `{app=~"api|worker"}`
  followed by `| env = "prod"` filters Crabka rows the same way Loki filters
  labels that existed before parser stages.
- The real-Loki parser differential corpus now also covers range-metric queries
  over parser pipelines. That caught and fixed the same fidelity gap for
  `matrix` responses: parser-extracted labels now participate in metric label
  sets before vector aggregation, matching Loki's pipeline label semantics.
- The real-Loki differential corpus now covers instant metric `/query` results
  as Loki `vector` payloads, pinning the route-level matrix-to-vector
  conversion used by Grafana and alerting clients.
- The JSON parser stage now preserves Loki parser-failure semantics for the
  covered stream path: invalid JSON lines remain in the result stream and carry
  `__error__="JSONParserErr"` plus Loki-compatible `__error_details__`, so
  parser failures can be selected by later pipeline filters. Successful parser
  output also follows Loki's `__error__ = ""` filter convention, allowing users
  to keep successfully parsed lines while dropping parser failures.
- Real-Loki differential coverage now includes every in-scope vector
  aggregation operator over range metrics: `sum by (...)`, `count without
  (...)`, `min by (...)`, `max by (...)`, and `avg without (...)`. These cases
  pin label reduction, negative grouping, and sample values against Loki for
  both count and byte metrics. In-process vector aggregation coverage now also
  includes Loki's population `stdvar` and `stddev` operators over grouped range
  metric samples, parameterized `topk` and `bottomk` selection that preserves
  original input series labels, and Loki's `approx_topk` function backed by
  Crabka's deterministic exact top-k selector.
- Real-Loki differential coverage now includes `bytes_over_time(...)` and
  `bytes_rate(...)` range metrics over compacted logs. This pinned byte-sample
  values and caught Loki's query-result fallback where streams without
  explicit level/severity labels still return `detected_level="unknown"` in
  stream and metric result labels, while metadata/index discovery keeps hiding
  that derived label.
- Metric hot/cold merge coverage now includes all in-scope range aggregations:
  `count_over_time(...)`, `rate(...)`, `bytes_over_time(...)`, and
  `bytes_rate(...)`. These tests prove uncompacted WAL rows contribute to
  metric samples while records at or before the compaction frontier are not
  double-counted.
- LogQL metric queries now include unwrapped-range slices for
  `rate`, `rate_counter`, `sum_over_time`, `avg_over_time`, `min_over_time`,
  `max_over_time`, `first_over_time`, `last_over_time`, and
  `stdvar_over_time` / `stddev_over_time`, plus parameterized
  `quantile_over_time` over extracted sample values. Metric queries also include
  synthetic `absent_over_time` samples for empty ranges. Raw signed integer,
  decimal, and scientific-notation unwraps are supported, plus Loki's `bytes(label)` unwrap conversion
  for byte literals such as `1KiB` and `duration(label)` /
  `duration_seconds(label)` conversion for duration literals such as `250ms`.
  Duration samples aggregate as fractional seconds, so `250ms + 500ms` returns
  `0.75`. Supported unwrapped range aggregations can group raw range samples
  before reduction with direct trailing `by(...)` / `without(...)` clauses. The
  documented range-vector `offset` modifier now shifts the sampled window while
  preserving the output evaluation timestamp. The
  unwrap stage marks non-numeric, non-byte, non-duration, or missing samples
  with `SampleExtractionErr`, allowing the existing
  `__error__=""` label filter to discard them before sample aggregation.
  Surviving non-empty `__error__` labels now reject metric queries with a
  Loki-style `bad_data` response instead of being returned as metric labels,
  covering both cold-block and hot-tail samples.
- Real-Loki differential coverage now includes the `/loki/api/v1/query` invalid
  LogQL parse path and invalid selector matchers on metadata routes
  (`labels`, `label/{name}/values`, and `series`). Loki returns a plain-text
  `400` parser message for these paths rather than the JSON Loki error envelope
  used by several validation errors, so Crabka now formats those parse failures
  with Loki-style line/column text. The corpus also pins `/query_range`
  `direction` validation: Loki returns plain text such as
  `invalid direction 'sideways'`, and Crabka now matches that route-level
  response instead of wrapping it in a JSON error envelope. `/query_range`
  metric requests with zero or negative `step` now use Loki's plain-text
  zero-or-negative-step response as well. Invalid `limit` values on `/query`
  now match Loki's plain-text `strconv.Atoi` parse-error response, while
  negative limits match Loki's `limit must be a positive value` response.
  Invalid timestamp query parameters such as `/query_range?start=not-a-number`
  now match Loki's plain-text `could not parse 'start' parameter` response.
  Invalid duration query parameters such as `/query_range?step=not-a-number`
  now match Loki's plain-text `cannot parse "..." to a valid duration`
  response. Invalid `since` durations such as `/query_range?since=-1` now
  match Loki's plain-text `could not parse 'since' parameter` response.
  `/query_range?interval=0` now matches Loki's successful no-op behavior, while
  negative intervals match Loki's plain-text `interval must be >= 0` response.
  Metadata-route invalid timestamp bounds such as `/series?start=not-a-number`
  are also pinned against Loki's plain-text parse response.
- The `| logfmt` parser now matches Loki for malformed quoted values in the
  covered case: fields before the malformed quote are still promoted, while the
  unterminated quoted field itself is skipped instead of being emitted with a
  synthetic value.
- The integration suite now includes a real Grafana container slice: Grafana's
  built-in Loki datasource is provisioned against a Crabka querier listener,
  its datasource health check succeeds, and a `query_range` request through the
  Grafana datasource proxy returns the compacted Crabka log row. The same test
  now also exercises the datasource proxy metadata paths Grafana uses for
  label-name, label-value, and stream-series exploration before executing the
  query through Grafana's backend `/api/ds/query` path, which is closer to
  Explore/panel execution and sends millisecond duration steps such as
  `1000ms`; Crabka accepts those Loki duration query parameters. HTTP query
  duration parameters now also accept ordered Prometheus-style compound
  durations such as `1m30s`, aligning `step`, `since`, and `interval` parsing
  with the LogQL range-selector duration parser.
  Crabka also accepts Loki's `vector(s)` scalar-vector function for instant
  query vectors and stepped query_range matrices, including arithmetic
  operators (`+`, `-`, `*`, `/`, `%`, `^`), parenthesized arithmetic groups,
  scalar literal operands, and filter / `bool` comparison operators between
  `vector(...)` terms. Instant `vector(...)` queries also cover Loki's
  logical/set `and`, `or`, and `unless` operators for the synthetic single-series
  vector path, and no-op `on(...)` / `ignoring(...)` vector matching modifiers
  with optional `group_left(...)` / `group_right(...)` group modifiers on
  arithmetic and comparison operators. That same path covers Grafana's Loki
  health-check expression `vector(1)+vector(1)`. Crabka also accepts Loki's
  `label_replace(...)` and `label_join(...)` functions over a synthetic
  single-series vector and over metric query vectors, including `$1`-style regex
  capture expansion and separator-joined source label values into output labels.
  The shared LogQL parser and `/format_query` endpoint also recognize
  `vector(...)`, `label_replace(metric_query, ...)`, and
  `label_join(metric_query, ...)` wrappers. Metric query vectors can also run scalar arithmetic and scalar
  comparisons with the scalar on either side using Loki's filtering comparison
  operators and `bool` sample replacement. Metric
  query vectors can also run vector/vector arithmetic, comparisons, and
  logical/set operators (`and`, `or`, `unless`) for instant vectors and stepped
  matrices, including exact label-set matching plus `on(...)` / `ignoring(...)`
  modifiers. Scalar-only `/loki/api/v1/query_range` expressions now also match
  Loki by returning stepped `matrix` samples with an empty metric label set
  instead of rejecting the range request.
  vector matching modifiers. Arithmetic and comparison metric queries also
  accept `group_left(...)` / `group_right(...)` group modifiers for many-to-one
  and one-to-many matches, including requested one-side label inclusion, default
  comparison filtering, and `bool` sample replacement. The shared parser plus
  `/format_query` endpoint recognize those shapes. Vector aggregations now also
  include `count_values("label", ...)` sample-value counting with optional
  grouping. Instant `/query` requests also accept pure scalar arithmetic
  expressions such as `1+1` and return Loki scalar results.
- Distributor, querier, and compactor routers now serve
  `/loki/api/v1/format_query` over GET and form-encoded POST, validating LogQL
  and normalizing the implemented stream selector/pipeline subset for Loki's
  all-components formatter endpoint. Real-Loki differential coverage now pins
  formatter response shape, field-filter operator spacing, and the
  `invalid-query` JSON envelope used for malformed or missing formatter queries.
- The querier router now serves `/loki/api/v1/index/volume` and
  `/loki/api/v1/index/volume_range`, returning Loki-style vector or matrix
  byte-volume responses from planned block metadata with tenant, selector,
  time-range, target-label, aggregation, limit, and form-POST support. Missing
  `start` bounds now follow Loki's recent-range default, with
  `volume_range` returning an instant-style vector in that case; missing `end`
  defaults to the current time and still honors Loki's built-in 30d1h volume
  query-length limit.
- The querier router now serves `/loki/api/v1/patterns`, grouping matching log
  lines into Loki-style structure patterns and step-bucket sample counts for
  Grafana's log-structure discovery workflows. The endpoint now accepts both
  GET query parameters and form-encoded POST bodies.
- The querier router now serves `/loki/api/v1/detected_fields` and
  `/loki/api/v1/detected_field/{name}/values` over GET and form-encoded POST,
  scanning planned matching rows for top-level JSON fields, logfmt pairs, and
  structured metadata with Loki-shaped field/value responses. The endpoints now
  also follow Loki's documented `start` / `end` / `since` time-window defaults
  and validate optional `step` duration parameters instead of silently ignoring
  them. Detected field type inference now also preserves Loki's documented
  `duration` and `bytes` field types for matching literal values instead of
  collapsing them to `string`. Stream query results also surface matched
  push-time structured metadata in the returned stream label set, matching
  Loki's query behavior.
- The first compactor crash-safety primitive now compacts positioned WAL records
  into deterministic object-store blocks and commits the partition offset only
  after the block and tenant index manifest are durable. Coverage now also
  reprocesses an uncommitted WAL batch after a simulated commit failure and
  asserts the restart rewrites the same block object bytes without adding a
  duplicate tenant-manifest block.
- A log block with Loki-shaped columns can be written as Parquet, registered in
  DataFusion, filtered with SQL equivalent to `{app="api"} |= "error"`, and
  serialized as Loki's `streams` JSON shape.

The spike found one concrete planning adjustment: production crates should let
DataFusion own the Arrow/Parquet crate line at the query boundary. DataFusion
54 currently re-exports Arrow/Parquet 58.x, while Crabka's workspace also has
Arrow 59 for streams-client work. Mixing both in the same binary compiles but
breaks Arrow downcasts at runtime because `arrow_array::Int64Array` from 59 is
not the same Rust type as DataFusion's `arrow_array::Int64Array` from 58.

## What was built

- `crates/blockstore/src/lib.rs`
  - `Labels` and stable `series_fingerprint(...)`
  - tenant-scoped `LabelIndex` with exact, negative, regex, and negative-regex
    predicates
  - `label_names` / `label_values` metadata lookups
  - `BlockKey`, `BlockDescriptor`, `BlockIndex`, and time-range pruning
  - deterministic object keys from `(tenant, partition, offsets, time-window)`
  - `LogRow`, `write_log_block(...)`, and `read_log_block(...)` for persisted
    Parquet log blocks with native Arrow map encoding for `structured_metadata`
  - `LogBlockTableProvider` and `register_log_blocks(...)` for registering
    planned local or object-store block files as a DataFusion table with
    explicit pushdown metadata
  - `register_log_blocks_from_object_store(...)` for sharing that DataFusion
    registration path with object-store-backed querier execution
- `crates/logql/src/lib.rs`
  - production `StreamQuery` AST
  - `LabelMatcher` with `=`, `!=`, `=~`, `!~`
  - ordered line-filter pipeline stages with `|=`, `!=`, `|~`, `!~`
  - `| json` / `| logfmt` parser stages and field comparison filters such as
    `status >= 500`
  - `MetricQuery`, `parse_metric_query(...)`, and the first range aggregation
    ASTs: `count_over_time(stream[range])`, `rate(stream[range])`, and
    `bytes_over_time(stream[range])`
  - vector aggregation ASTs for `sum` / `count` / `min` / `max` / `avg` with
    `by(...)` and `without(...)` grouping
  - `parse_query(...)` and `StreamQuery::matches(...)`
  - `plan_stream_query(...)` lowering selector matchers to the blockstore
    indexes and returning candidate blocks
- `crates/observability/src/lib.rs`
  - role-selectable service config for `--target distributor|compactor|querier`
    plus `--listen-addr`, `--object-store-url`, `--wal-bootstrap-server`,
    `--wal-topic`, and querier index source config for local manifests, tenant
    object-store manifests, and tenant object-store shard catalogs
  - typed `Role` enum, `ServiceDependencies`, `build_service_dependencies(...)`,
    and `build_service_router(...)` for assembling distributor and querier
    runtime dependencies / HTTP surfaces from the same config
  - `serve_service(...)` / `serve_service_listener(...)` for binding and serving
    the role-specific HTTP router
  - `KafkaLogWalSink` and `build_kafka_wal_record(...)` for producing
    tenant/series-keyed JSON WAL records through `crabka-client-producer` with
    `acks=all`
  - `decode_kafka_wal_record(...)`, `KafkaWalRecord`, and
    `compact_kafka_wal_records_to_object_store(...)` for turning consumed Kafka
    value bytes plus partition/offset metadata into crash-safe compaction
    batches
  - `KafkaWalHeader` and native Kafka log-line decoding for records whose
    values are log bodies and whose headers carry `crabka-tenant`,
    `crabka-log-timestamp-ns`, `crabka-log-label-*`, and optional
    `crabka-log-metadata-*`
  - `LogWalConsumer`, `KafkaLogWalConsumer`, and
    `compact_next_kafka_wal_batch_to_object_store(...)` for the async
    poll/compact/commit boundary the compactor role will run in a loop
  - `run_compactor_once(...)` and compactor `ServiceDependencies` wiring for a
    service-config-driven compactor iteration over an injected or configured
    WAL consumer and object store
  - `run_compactor_until_idle(...)`, `run_compactor_until_shutdown(...)`, and
    compactor `serve_service(...)` dispatch for restart-aware, multi-batch
    compaction while serving the compactor HTTP router
  - `distributor_router(...)`, `WalLogRecord`, and `LogWalSink` for Loki JSON
    and snappy-compressed protobuf push normalization into tenant-scoped WAL
    records
  - `--max-ingest-body-bytes` for rejecting oversized distributor HTTP ingest
    requests with Loki-style `rate_limited` errors before WAL append
  - `--wal-append-timeout-ms` for bounding stalled distributor WAL appends and
    returning Loki-style `503` / `server_error` instead of hanging under
    backpressure
  - `LogIngestLimiter` / `IngestLimitError` for rejecting normalized
    tenant-scoped HTTP ingest batches with Loki-style `429` / `rate_limited`
    errors and OTLP gRPC batches with `ResourceExhausted` before WAL append
  - Loki stream-label and structured-metadata name validation for JSON/protobuf
    push payloads before WAL append
  - `POST /v1/logs` and `POST /otlp/v1/logs` OTLP/HTTP JSON and protobuf
    normalization plus OTLP/gRPC logs service normalization into the same
    tenant-scoped WAL record path
  - `execute_stream_query(...)` for cold-block LogQL stream plans to Loki
    `streams` JSON
  - `execute_stream_query_with_hot_tail(...)` for merging cold blocks with
    uncompacted WAL rows after a compaction frontier
  - `execute_metric_query(...)` / `execute_metric_query_range(...)` for the
    first cold-block LogQL metric plans to Loki `matrix` JSON, including
    vector aggregation grouping over stepped samples
  - `execute_metric_query_range_with_hot_tail(...)` for merging cold and hot
    rows in the first metric range path
  - `BufferedLogHotTail` and `poll_log_hot_tail_once(...)` for turning consumed
    Kafka WAL batches into the same hot-tail interface used by query/tail routes
  - `SharedCompactionFrontier`, `ServiceDependencies::with_hot_tail_frontier(...)`,
    `ServiceDependencies::with_hot_tail_shared_frontier(...)`,
    `ServiceDependencies::with_compaction_frontier(...)`, and
    `ServiceDependencies::with_wal_consumer(...)` for wiring hot-tail sources and
    compactor progress into query/compaction service startup
  - `write_compaction_frontier_to_object_store(...)` and
    `read_compaction_frontier_from_object_store(...)` for durable compactor
    frontier manifests under the object-store index prefix and cross-process
    querier hot-tail filtering
  - live-broker integration coverage for `KafkaLogWalSink` and
    `KafkaLogWalConsumer` against an in-process Crabka broker, plus
    config-built distributor, compactor, and querier coverage over the same
    broker through full push → compact → query, hot/cold-frontier, and compactor
    restart loops
  - `build_service_dependencies(...)` now builds configured Kafka WAL clients for
    distributor, compactor, and querier roles, rejecting missing WAL bootstrap
    config for each WAL-backed startup path
  - `execute_stream_query_from_object_store(...)` for scanning planned
    object-store block payloads through the DataFusion table-provider bridge
  - `execute_tail_query(...)` plus `GET /loki/api/v1/tail` websocket upgrade
    support for continuous Loki hot-tail stream frames from the configured
    `LogHotTail`
  - `--max-query-range-ns`, `--max-query-series`, `--max-query-bytes`,
    `--max-query-length`,
    `QuerierState::with_max_query_range_ns(...)`, and
    `QuerierState::with_max_query_series(...)`,
    `QuerierState::with_max_query_bytes(...)`, and
    `QuerierState::with_max_query_length(...)` for enforcing range, series,
    planned-byte, and decoded-LogQL-length query limits at HTTP query entry
  - `loki_router(...)` and `QuerierState` for `GET /loki/api/v1/query`,
    `GET /loki/api/v1/query_range`, metadata routes, and `tail`
- `crates/observability-spike/src/lib.rs`
  - `Labels = BTreeMap<String, String>` canonical label sets
  - `series_fingerprint(labels)` using xxh3 over length-prefixed sorted labels
  - `LabelIndex` with `(label_name, value) -> series fingerprints` postings
  - `LabelIndex::label_names` / `label_values` for Loki label metadata endpoints
  - `LogSelector` with matcher semantics and one optional line filter
  - `loki_streams_response(...)` producing Loki-compatible stream JSON
- `crates/observability-spike/examples/loki_spike.rs`
  - writes three rows to a Parquet file
  - registers the file with DataFusion
  - executes:

```sql
select timestamp_ns, line
from logs
where app = 'api' and line like '%error%'
order by timestamp_ns
```

  - emits Loki `status/data/resultType/result` JSON

This is intentionally throwaway. It is proof material for the production
`crabka-blockstore` / `crabka-logql` / `crabka-observability` plan, not the
first production slice.

## Evidence

Core behavior tests:

```text
cargo test -p crabka-observability-spike --test core
running 7 tests
test fingerprint_is_stable_for_label_order ... ok
test invalid_logql_reports_a_parse_error ... ok
test label_index_prunes_to_matching_series ... ok
test label_index_serves_label_metadata ... ok
test loki_response_groups_lines_by_stream ... ok
test parsed_logql_supports_matchers_and_line_filters ... ok
test parsed_logql_supports_regex_matchers_and_negative_line_filters ... ok
```

Production LogQL parser tests:

```text
cargo test -p crabka-logql
running 14 parser tests
test invalid_regex_reports_parse_error ... ok
test invalid_syntax_reports_expected_token ... ok
test parses_bytes_over_time_metric_query ... ok
test parses_count_over_time_metric_query ... ok
test parses_json_parser_stage_and_numeric_field_filter ... ok
test parses_logfmt_parser_stage_and_string_field_filter ... ok
test parses_multiple_line_filters_in_order ... ok
test parses_rate_metric_query ... ok
test parses_selector_with_all_matcher_ops ... ok
test parses_vector_aggregation_metric_query ... ok
test parses_vector_aggregation_without_metric_query ... ok
test query_evaluator_applies_json_parser_stage_and_field_filter ... ok
test query_evaluator_applies_logfmt_parser_stage_and_field_filters ... ok
test query_evaluator_applies_matchers_and_pipeline ... ok
running 2 planner tests
test stream_planner_keeps_regex_and_negative_matchers_in_index_filter ... ok
test stream_planner_prunes_series_and_blocks_before_line_filters ... ok
```

Production blockstore tests:

```text
cargo test -p crabka-blockstore
running 22 tests
test block_index_prunes_by_tenant_time_and_fingerprint ... ok
test datafusion_table_rejects_empty_block_list ... ok
test datafusion_table_scans_only_planned_log_blocks ... ok
test log_block_table_provider_exposes_planned_filter_pushdown ... ok
test log_block_table_provider_scans_planned_object_store_blocks ... ok
test deterministic_block_keys_encode_compactor_idempotency_fields ... ok
test label_index_is_tenant_scoped_and_applies_all_matcher_ops ... ok
test label_metadata_is_tenant_scoped ... ok
test log_index_manifest_round_trips_label_and_block_indexes ... ok
test log_index_manifest_round_trips_through_object_store ... ok
test parquet_log_block_object_path_is_prefix_and_block_key ... ok
test parquet_log_block_rejects_rows_outside_key_time_range ... ok
test parquet_log_block_round_trips_rows_sorted_by_series_and_timestamp ... ok
test parquet_log_block_round_trips_through_object_store ... ok
test parquet_log_block_writes_structured_metadata_as_arrow_map ... ok
test series_fingerprint_is_stable_across_label_ordering ... ok
test tenant_log_index_manifest_object_path_is_tenant_prefixed ... ok
test tenant_log_index_manifest_round_trips_only_one_tenant ... ok
test tenant_log_index_shard_catalog_object_path_is_tenant_prefixed ... ok
test tenant_log_index_shard_catalog_selects_overlapping_shards_and_merges_indexes ... ok
test tenant_log_index_shard_manifest_object_path_is_tenant_and_time_prefixed ... ok
test tenant_log_index_shard_round_trips_only_matching_time_and_series ... ok
```

Production service-role tests:

```text
cargo test -p crabka-observability
running 91 tests
test compactor_commits_partition_offset_after_writing_block_and_index ... ok
test compactor_decodes_kafka_wal_records_before_writing_block ... ok
test compactor_decodes_native_kafka_log_records_from_headers ... ok
test compactor_does_not_commit_polled_batch_when_decode_fails ... ok
test compactor_does_not_commit_offset_for_invalid_kafka_wal_payload ... ok
test compactor_does_not_commit_offset_for_invalid_wal_batch ... ok
test compactor_polls_kafka_wal_batch_then_commits_after_object_store_write ... ok
test compactor_once_loads_existing_manifest_after_restart ... ok
test compactor_runtime_compacts_one_polled_batch_from_service_config ... ok
test compactor_runtime_advances_shared_compaction_frontier_after_commit ... ok
test compaction_frontier_round_trips_through_object_store ... ok
test compactor_runtime_keeps_polling_after_idle_until_shutdown ... ok
test compactor_runtime_loads_existing_manifest_after_restart ... ok
test compactor_runtime_preserves_indexes_across_polled_batches_until_idle ... ok
test compactor_runtime_reloads_shared_frontier_after_restart ... ok
test compactor_runtime_rejects_missing_object_store ... ok
test compactor_runtime_rejects_missing_wal_consumer_dependency ... ok
test compactor_service_target_keeps_running_after_idle ... ok
test compactor_writes_block_then_tenant_index_manifest ... ok
test configured_object_store_metric_query_returns_partial_warning_for_missing_block ... ok
test configured_object_store_query_returns_partial_warning_for_missing_block ... ok
test empty_stream_plan_returns_empty_loki_streams_result ... ok
test executes_avg_without_vector_aggregation_with_stepped_matrix_samples ... ok
test executes_bytes_over_time_query_with_stepped_matrix_samples ... ok
test executes_count_min_and_max_vector_aggregations ... ok
test executes_count_over_time_merging_cold_blocks_with_hot_wal_tail ... ok
test executes_count_over_time_query_as_loki_matrix_json ... ok
test executes_count_over_time_query_with_stepped_matrix_samples ... ok
test hot_tail_buffer_polls_and_decodes_kafka_wal_records ... ok
test kafka_wal_record_encodes_tenant_series_key_headers_and_json_payload ... ok
test kafka_wal_record_decode_rejects_invalid_payload ... ok
test kafka_wal_record_decodes_payload_with_consumed_position ... ok
test executes_rate_query_with_stepped_matrix_samples ... ok
test executes_sum_by_vector_aggregation_with_stepped_matrix_samples ... ok
test executes_stream_query_filters_hot_tail_by_partition_offset_frontier ... ok
test executes_stream_query_over_object_store_blocks_as_loki_json ... ok
test executes_stream_query_over_planned_cold_blocks_as_loki_json ... ok
test executes_stream_query_merging_cold_blocks_with_hot_wal_tail ... ok
test executes_stream_query_with_json_field_filter_over_structured_metadata ... ok
test executes_stream_query_with_logfmt_field_filter_over_line_body ... ok
test executes_tail_query_filters_hot_tail_by_partition_offset_frontier ... ok
test executes_tail_query_over_hot_wal_tail_as_loki_streams_json_frame ... ok
test labels_endpoint_returns_tenant_label_names ... ok
test label_values_endpoint_returns_tenant_values ... ok
test loki_push_endpoint_accepts_snappy_protobuf_payloads ... ok
test loki_push_endpoint_rejects_invalid_snappy_protobuf_without_wal_append ... ok
test loki_push_endpoint_rejects_invalid_timestamp_without_wal_append ... ok
test loki_push_endpoint_writes_tenant_scoped_wal_records ... ok
test otlp_grpc_logs_service_rejects_missing_tenant_without_wal_append ... ok
test otlp_grpc_logs_service_writes_tenant_scoped_wal_records ... ok
test otlp_logs_endpoint_accepts_protobuf_payloads ... ok
test otlp_logs_endpoint_rejects_invalid_protobuf_without_wal_append ... ok
test otlp_logs_endpoint_rejects_invalid_timestamp_without_wal_append ... ok
test otlp_logs_endpoint_writes_tenant_scoped_wal_records ... ok
test object_store_metric_query_returns_partial_result_with_warning_for_unreadable_block ... ok
test object_store_stream_query_returns_partial_result_with_warning_for_unreadable_block ... ok
test parses_compactor_wal_consumer_config ... ok
test parses_distributor_wal_config ... ok
test parses_explicit_service_targets ... ok
test parses_querier_object_store_shard_catalog_config ... ok
test parses_querier_wal_tail_config ... ok
test querier_dependencies_require_wal_bootstrap_server ... ok
test query_endpoint_can_load_indexes_from_persisted_manifest ... ok
test query_endpoint_can_build_querier_from_object_store_shard_catalog_config ... ok
test service_router_builds_configured_local_object_store_for_querier_role ... ok
test service_router_loads_persisted_frontier_for_configured_querier_hot_tail ... ok
test query_endpoint_can_load_tenant_index_from_object_store_manifest ... ok
test query_endpoint_can_load_tenant_index_from_object_store_shard_catalog ... ok
test query_endpoint_can_load_tenant_index_from_object_store_shard ... ok
test query_endpoint_uses_updated_shared_compaction_frontier_for_hot_tail ... ok
test query_endpoint_applies_backward_direction_before_limit ... ok
test query_endpoint_applies_limit_to_stream_results ... ok
test query_endpoint_merges_cold_blocks_with_hot_wal_tail ... ok
test query_endpoint_rejects_invalid_direction ... ok
test query_endpoint_rejects_missing_tenant_header ... ok
test query_endpoint_returns_loki_error_for_invalid_logql ... ok
test query_endpoint_returns_loki_streams_json_for_tenant ... ok
test query_range_endpoint_applies_start_end_and_tenant ... ok
test query_range_endpoint_applies_step_for_count_over_time_matrix_json ... ok
test query_range_endpoint_returns_loki_error_for_invalid_step ... ok
test query_range_endpoint_returns_count_over_time_matrix_json ... ok
test rejects_missing_target ... ok
test rejects_unknown_target ... ok
test series_endpoint_applies_matchers_time_range_and_tenant ... ok
test series_endpoint_returns_loki_error_for_invalid_time_bound ... ok
test service_listener_serves_distributor_role_on_bound_tcp_listener ... ok
test service_router_builds_distributor_role ... ok
test service_router_builds_querier_role_with_hot_tail_dependency ... ok
test service_router_builds_querier_role_with_wal_consumer_hot_tail_poller ... ok
test service_router_builds_querier_role_from_object_store_shard_catalog_config ... ok
test tail_endpoint_streams_hot_wal_tail_over_websocket ... ok
```

Empirical DataFusion/Parquet run:

```text
cargo run -p crabka-observability-spike --example loki_spike
{
  "data": {
    "result": [
      {
        "stream": {
          "app": "api",
          "env": "prod"
        },
        "values": [
          [
            "20",
            "error: boom"
          ]
        ]
      }
    ],
    "resultType": "streams"
  },
  "status": "success"
}
GO: parsed LogQL -> Parquet block -> DataFusion filter -> Loki streams JSON returned 1 row(s)
```

Resolved dependency versions from the spike:

| Crate | Version | Notes |
|---|---:|---|
| `datafusion` | 54.0.0 | current crates.io release probed by `cargo info datafusion` |
| DataFusion re-exported `arrow` | 58.3.0 | visible in the lockfile during DataFusion compilation |
| workspace `arrow` | 59.x | already used by streams-client columnar support |
| `regex` | 1.12.4 | used for spike-grade `=~`/`!~` and `|~`/`!~` evaluation |
| `xxhash-rust` | 0.8.15 | used for spike-grade stream fingerprints |

## Findings

### 1. Label indexing is straightforward

The design's two-level index remains credible. A postings map from
`(label_name, value)` to fingerprints is enough to answer the first planning
question: selector pruning can happen before row scanning. The production
`crabka-blockstore` slice now adds matcher operators (`!=`, `=~`, `!~`) and
tenant/time/block pruning. Persistence is now covered by versioned local,
tenant object-store, and tenant shard manifests that serialize the series
dictionary and block descriptors, then rebuild postings when loaded into the
querier cache.

### 2. Stream fingerprinting should be our own API-compat hash at first

The spike uses xxh3 over length-prefixed, sorted labels. That gives stable,
fast, deterministic fingerprints without coupling object-store indexes to
Loki's internal hash implementation. This supports the original design's note:
matching Loki's fingerprint only matters for index-file/tooling interop, not
for HTTP API compatibility. Recommendation: ship Crabka-owned fingerprints in
the MVP, and revisit Loki fingerprint parity only if we decide to read/write
Loki-native index files.

### 3. Production Parquet log blocks use the DataFusion Arrow line

The production `crabka-blockstore` crate now writes and reads Parquet blocks
with `datafusion::arrow` and `datafusion::parquet` re-exports. Rows are sorted
by `(series_fingerprint, timestamp_ns)` before writing, and block descriptors
derive their fingerprint set from the actual rows written. This moves the
columnar block format out of the throwaway spike while keeping the generic index
types available to planners.

The persisted block slice stores structured metadata as a native Arrow
`Map<Utf8, Utf8>` column named `structured_metadata`, matching the design's
high-cardinality per-line attribute shape while preserving the existing
`BTreeMap<String, String>` Rust API at block boundaries.

### 4. Planned blocks can feed DataFusion scans

The production `crabka-blockstore` crate now exposes `LogBlockTableProvider`
and `register_log_blocks(...)`, which build a custom DataFusion table provider
from the exact `BlockDescriptor` set selected by the planner. The provider wraps
DataFusion's multi-path Parquet listing scan for local files, can scan planned
Parquet blocks directly through `object_store::ObjectStore`, preserves the
planned block set, and advertises inexact pushdown for the scan columns the
planner owns: `series_fingerprint`, `timestamp_ns`, and `line`. Stream-query
and metric-query execution now build deterministic DataFusion scan SQL shapes
that include the active scan time range, planned
`series_fingerprint in (...)` predicate, and pushdown for literal `|=` / `!=`
line filters while leaving regex and parser/field filters as post-scan
correctness checks. Metric scans derive that time range from the evaluation
window plus the range selector before reading local or object-store blocks.

### 5. DataFusion works for the wedge's first query shape

DataFusion can read the Parquet block and execute the first useful LogQL shape:
label equality plus a substring line filter. In the production planner, the
SQL/DataFusion expression equivalent is:

```text
app = 'api' AND line LIKE '%error%'
```

The line filter in production should be pushed below projection and paired with
block/row-group pruning where possible. Bloom filters are still a later
optimization.

### 6. Cold-block streams results now have Loki JSON shape

The production `crabka-observability` crate now executes a planned stream query
against planned cold Parquet blocks and serializes Loki-compatible
`status/data/resultType/result` JSON. The executor uses the blockstore's series
dictionary to map fingerprints back to stream labels, applies the current LogQL
stream predicate, and groups matching rows by stream label set.

The same crate now wires that executor behind Axum routes for
`/loki/api/v1/query` and `/loki/api/v1/query_range`. The route layer reads
`X-Scope-OrgID` for the tenant, parses the LogQL `query` parameter, computes the
time range from `time` or `start`/`end`, applies `direction` and `limit` to
stream results, and returns Loki-shaped JSON.

The router also covers the first metadata endpoints:

- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/label/{name}/values`
- `GET /loki/api/v1/series`

`labels` and `label/{name}/values` are served directly from the tenant-scoped
label index and now apply optional `query` selector filters plus `start`/`end`
bounds through the block index. `series` accepts Loki's `match[]` query
parameter, applies optional `start`/`end` bounds through the planner, and returns
only label sets with candidate blocks in the selected time range. `tail` upgrades
to a websocket, emits a Loki-shaped snapshot frame, and keeps polling the
configured hot WAL tail for newly appended matching records; malformed and
missing tail query websocket handshakes now match Loki's pre-upgrade text error
responses under real-Loki differential coverage. Query and metadata
route errors now return the Loki-style JSON error envelope with `bad_data` for
client-side query errors and `server_error` for server-side failures. The query,
range-query, and tail routes now parse scalar URL params through that envelope
too, so malformed `time`, `start`, `end`, `since`, `step`, `interval`, `limit`,
or `direction` values do not fall back to framework-generated plain-text
rejections. The distributor and querier routers also expose Loki's common status
endpoints: `/ready` returns the readiness body expected by probes,
GET `/log_level` returns Loki's current-level message payload, and `/metrics`,
`/config`, and `/services` return stable component status payloads. `/services`
uses Loki's newline-delimited `service => Running` text shape; real Loki does
not guarantee service order, so differential coverage normalizes the line set.
`/config` keeps a minimal YAML placeholder but now matches real Loki's stable
top-level `target: all` line.
`/metrics` keeps Crabka's service-up gauge and now also exposes stable
Loki-compatible Prometheus families for `loki_build_info` and
`loki_boltdb_shipper_compactor_running`.
`/log_level` also accepts the documented POST parameter/body forms for changing
levels at the HTTP boundary, returning Loki's `status` / `message` success
envelope, while invalid or missing levels use Loki's `failed` / `message` error
envelope, and
`/loki/api/v1/status/buildinfo` returns the documented build-info object fields
with real-Loki differential coverage for the response shape.
The distributor router also serves `/distributor/ring` with an HTML status page
that reports the current single-process distributor as an `ACTIVE` ring member.
`build_service_router` can now construct the compactor HTTP router for the same
common status/build-info endpoints, and that router serves `/compactor/ring`
with the same Loki `Ring Status` / `ACTIVE` page shape for the current
single-process compactor surface. The runtime `compactor` service target now
binds the listener, serves that HTTP surface, and runs the long-polling
compaction loop concurrently.
The compactor HTTP surface also accepts Loki delete requests at
`/loki/api/v1/delete`, tracks active requests, scopes listing and cancelation by
`X-Scope-OrgID`, validates LogQL plus Unix-second time bounds, and persists the
registry under `data_root` for configured local compactor and querier roles.
When the same delete registry is attached to a querier, stream, metric, tail,
pattern, and detected-field queries now apply those active requests as
query-time tombstones and suppress matching rows before returning streams,
calculating metric samples, streaming live tail frames, or aggregating discovery
output. Compaction now applies the same active registry when writing new WAL
batches into blocks, omitting matching rows while still committing the full
compacted offset range. The compactor maintenance path also rewrites overlapping
existing local manifests, object-store tenant manifests, and object-store
shard-catalog blocks for active delete requests and omits fully deleted blocks
from the rewritten manifests.
Real-Loki differential coverage records that the default Loki 3.4.2 container
does not mount `/loki/api/v1/delete`, returning `404 page not found`, while
Crabka intentionally serves create/list/cancel lifecycle operations because the
delete registry is implemented and wired into compactor and querier paths.
The querier also exposes read-only ruler inventory responses at
`/loki/api/v1/rules`, `/prometheus/api/v1/rules`, and
`/prometheus/api/v1/alerts`, covering Grafana/Loki clients that probe rule and
alert inventory even when Crabka has no configured ruler backend. Real-Loki
differential coverage pins Loki's default no-rule-dir response for the native
rules endpoint and the empty Prometheus rules/alerts success envelopes,
including their empty `errorType` and `error` fields. The deprecated
`/api/prom/rules` root alias follows the native Loki rules response, while
`/api/prom/alerts` remains unmounted and returns Loki's plain-text
`404 page not found` body. `/ruler/ring` now returns
a Loki-compatible `Cortex Ruler Status` HTML page for the current in-process
ruler API surface.
The querier also handles
`/loki/api/v1/index/stats`, using the existing selector planner to report
stream, chunk, entry, and byte counts for local or object-store cold blocks.
The routers also expose Loki's deprecated `/api/prom/*` aliases for older
clients: `/api/prom/push`, `/api/prom/query`, `/api/prom/tail`,
`/api/prom/label`, `/api/prom/label/{name}/values`, and `/api/prom/series`.
Real-Loki differential coverage now records that the default Loki 3.4.2
container returns `404 page not found` for `/api/prom/query_range`, while
Crabka intentionally keeps the deprecated range-query alias available for older
clients. The same corpus pins `/api/prom/query`'s legacy stream-only behavior:
instant stream results are served, while instant metric/vector results return
Loki's plain-text `400` response. It also pins `/api/prom/label`'s legacy
`values` response and `/api/prom/series`'s canonical `status` / `data` response
shape.
The `query`, `query_range`, `series`, and `index/stats` routes accept the
documented form-encoded POST body path in addition to GET query parameters.
The `query` and `query_range` routes also attach Loki's documented `data.stats`
object to HTTP responses. Stream responses now populate conservative planned
cold-block byte/chunk counters and returned line totals, while metric responses
populate planned cold-block byte/chunk counters and returned metric-sample
totals. Stream responses split returned line totals between Loki's `store`
bucket for cold-block reads and `ingester` bucket for matching hot-tail records.
Metric responses now make the same source split for returned samples by matching
hot-tail-derived metric samples back to the Loki vector or matrix response.
Metadata routes now union cold label-index series with uncompacted hot WAL-tail
records after the current frontier, so recently ingested labels and streams are
visible to `labels`, `label/{name}/values`, and `series` before compaction.
Stream queries now default to Loki's `direction=backward` semantics before
applying `limit`, while explicit `direction=forward` and invalid-direction
errors keep their existing behavior.
Timestamp query params now follow Loki's documented timestamp formats for
`time`, `start`, and `end`: integer Unix nanoseconds remain supported, and the
HTTP parser also accepts fractional Unix seconds plus RFC3339/RFC3339Nano
strings.
`query_range` defaults missing bounds to Loki's documented recent window,
ending at current time and starting one hour earlier unless `since` derives a
different start.
`since` query params derive `start` from `end` when `start` is absent, covering
range queries and label/series metadata routes that use Loki time bounds.
`query` and `query_range` also support Loki's pipe-separated multi-tenant query
header form, merging per-tenant stream or metric results and scan statistics
into one response.
`step` query params likewise accept Loki's documented float-second and
Prometheus-duration forms while preserving the existing integer nanosecond form.
Stream `query_range` responses also honor Loki's `interval` parameter to sample
returned log entries before direction and limit are applied.
The distributor, querier, and compactor routers now also expose Loki's
`format_query` endpoint over GET and form-encoded POST. It validates LogQL,
formats the implemented stream-query subset including regex field filters,
preserves Loki's compact field-filter operator spacing, and returns the Loki
`invalid-query` error envelope for invalid or missing formatter expressions.
Real-Loki differential coverage pins that success and error response shape. Query-route
parse failures and metadata-route selector parse failures follow Loki's
plain-text `400` parse-error response instead of the JSON envelope, including
Loki's empty-query parse response when required `query` parameters are absent.
The router also exposes Loki's `index/volume` and `index/volume_range`
endpoints. These currently estimate byte volume from planned block metadata,
honor the selector/time/tenant filters and basic `targetLabels`, `aggregateBy`,
and `limit` controls, and return Loki-style vector or matrix envelopes with
query stats. Real-Loki differential coverage now pins `index/stats` success
response shape, instant `index/volume`
and target-label `index/volume_range` response shapes, including grouped series
labels and the presence of the `data.stats` object while normalizing
implementation-specific stats and volume byte accounting. Differential coverage also
pins `index/volume_range` invalid `step` errors, including Loki's plain-text
zero-step and invalid-duration responses, and both index-volume endpoints now
match Loki's plain-text invalid-aggregation response. Malformed LogQL selectors
on `index/stats`, `index/volume`, and `index/volume_range` also now match
Loki's plain-text parse-error response.
The router now also exposes Loki's `patterns` endpoint, using the existing
selector planner and row matcher before grouping matching lines into detected
structure patterns with step-bucket sample counts. Real-Loki differential
coverage records that the default Loki 3.4.2 container returns `404` for
`/loki/api/v1/patterns`, while Crabka intentionally serves the endpoint for
Grafana-style log-structure discovery.
The router now also exposes Loki's detected-field discovery endpoints over GET
and form-encoded POST. `/loki/api/v1/detected_fields` reports discovered field
names, inferred types, cardinalities, and parser sources; `/loki/api/v1/detected_field/{name}/values`
returns unique sorted field values. Both reuse the existing selector planner and
row matcher before scanning JSON, logfmt, and structured metadata fields, and
their parameter parser supports Loki's documented `since`-derived default time
window and optional `step` duration validation. Differential coverage now pins
detected-field invalid `step` errors, including Loki's plain-text zero-step and
invalid-duration responses, and malformed detected-field LogQL now follows
Loki's plain-text parse-error response on both detected-field endpoints,
including the empty-query parse response when `query` is absent.
Detected type inference covers
boolean, int, float, string, duration, and bytes values. Real-Loki differential
coverage now pins detected-field discovery for JSON and logfmt rows, including
Loki's generated `detected_level` field with `parsers: null` and matching
detected field values.
Remaining HTTP work includes endpoint-by-endpoint Loki error message/status
parity, deeper query language coverage, broader Grafana datasource coverage,
and differential-vs-Loki coverage.

`crabka-blockstore` now persists the cold-path index as a versioned JSON
manifest. The manifest stores the series dictionary and block descriptors;
loading rebuilds the derived postings index and rejects fingerprint mismatches.
It has both local filesystem helpers for tests/fixtures and async
`object_store::ObjectStore` helpers for the real S3/GCS-style backend. A
`LocalFileSystem` object-store test proves the same trait boundary is exercised
without depending on a cloud service.

The object-store path now also supports tenant-prefixed manifests at
`tenant=<id>/index/logs/manifest.json` and time-sharded tenant manifests at
`tenant=<id>/index/logs/shards/time=<start>-<end>/manifest.json`. Writing a
tenant manifest filters the series dictionary and block descriptors to that
tenant only; writing a shard filters further to the blocks overlapping the shard
range and the series fingerprints those blocks reference. Reading applies the
same tenant filter defensively. `QuerierState::from_manifest` can start the HTTP
querier from persisted local metadata, `QuerierState::from_tenant_object_store`
can start it from a tenant-scoped object-store manifest, and
`QuerierState::from_tenant_object_store_shard` can start it from one tenant
time shard. `crabka-blockstore` also writes a shard catalog at
`tenant=<id>/index/logs/shards/manifest.json`, and
`read_tenant_log_index_shards_from_object_store` selects overlapping shard
manifests for a query time range and merges their label/block indexes without
duplicating deterministic block keys. `QuerierState::from_tenant_object_store_shards`
uses that catalog/range loader for the HTTP path. `ServiceConfig` and
`build_querier_state` now expose this as a startup choice alongside local
manifests and tenant object-store manifests. The service is no longer limited
to indexes assembled in the same process that writes blocks. The real compactor
loop now updates the tenant shard catalog incrementally as it writes durable
object-store blocks, so a shard-catalog querier can discover newly compacted
blocks without a prebuilt fixture. Remaining persistence work is deeper
compactor/catalog hardening around overlapping shard policies and crash-window
validation.

Parquet log blocks now have the same object-store boundary. `crabka-blockstore`
can write a complete block payload to `object_store::ObjectStore` using the
deterministic `BlockKey` object key, and read it back through the Parquet reader.
The local filesystem block helpers remain for DataFusion listing-table tests,
while the object-store helpers are the compactor/store-gateway path for S3/GCS.

`crabka-observability` now has the first compactor-facing primitive:
`compact_log_block_to_object_store`. It writes the Parquet block payload first,
updates the block index, then persists the tenant manifest. This matches the
design's durability ordering up to the object-store boundary.
`compact_wal_records_to_object_store` stages label/block index updates, writes
the deterministic block plus tenant manifest, and only then calls a partition
offset committer with the compacted `WalPosition`. Invalid WAL batches do not
advance caller indexes or commit offsets. `run_compactor_until_shutdown` wraps
that primitive in a long-running loop that continues after idle polls and exits
only when its shutdown future fires; the `compactor` service target uses that
loop while serving the compactor HTTP router. The query and tail hot-paths also have a
`CompactionFrontier` that can filter uncompacted WAL records by timestamp or by
per-partition committed offset. `SharedCompactionFrontier` now lets the compactor
advance that frontier after a durable batch commit while querier query/tail paths
snapshot it at execution time, closing the in-process compactor-to-querier
frontier handoff. The frontier is persisted under the object-store index prefix
and reloaded during compactor startup and configured querier hot-tail startup,
so restart paths preserve committed per-partition offsets and cross-process
query startup avoids hot/cold duplication. The compactor runtime also splits
mixed-tenant WAL polls into tenant/partition chunks before writing object-store
blocks, preserving tenant manifests while still committing offsets only after
the durable writes complete. If the offset commit fails after those durable
writes, replaying the same WAL offset rewrites the deterministic object key and
idempotently replaces the block descriptor in the tenant manifest instead of
adding a duplicate entry. The long-running compactor loop also retries
transient object-store errors when writing the compaction-frontier manifest after
a committed batch, preserving the cross-process frontier handoff through brief
store outages.

The querier now has matching object-store execution primitives:
`execute_stream_query_from_object_store` and
`execute_metric_query_from_object_store`. Given a planned `StreamPlan`, they read
the selected Parquet blocks via `object_store::ObjectStore`, apply the same
tenant label lookup and LogQL predicates as the local DataFusion path, and
serialize Loki-compatible `streams` or `matrix` JSON. This route-level execution
is still a row-oriented bridge over the now-available DataFusion object-store
table provider, but it proves that tenant manifests, object keys, block payloads,
planning, and Loki serialization can compose across the object-store boundary. If a selected
object-store block cannot be read, the primitives now skip that block, keep
readable results, and add a Loki-style `warnings` array naming the skipped block.
Configured `--object-store-url` querier startup now keeps the object-store handle
in `QuerierState`, and stream plus metric query routes use it for cold block
reads, so the same partial-result behavior reaches HTTP routes. Tenant-manifest
and shard-catalog queriers are no longer limited to one process-level tenant:
when `--tenant` is omitted, role startup keeps a request-scoped object-store
index source and resolves the tenant manifest or overlapping tenant shards from
`X-Scope-OrgID` before planning stream queries or metadata responses.

### 7. Minimal LogQL can start smaller than the design's whole subset

The production `crabka-logql` crate now covers:

- label matchers: `=`, `!=`, `=~`, `!~`
  - regex label matchers are anchored like Loki/Prometheus selectors
  - `=~` follows Loki/Prometheus absent-label behavior for regexes that can
    match the empty string when another matcher keeps the selector selective
  - selectors must contain at least one matcher that does not match the empty
    string, matching Loki/Prometheus validation for broad selectors
- ordered line-filter stages: `|=`, `!=`, `|~`, `!~`
- `| json` parser stage for nested object fields flattened with `_`, sanitized
  label names, and skipped arrays
- `| logfmt` parser stage for line-body fields
- `_extracted` suffixing when parser-extracted labels collide with existing
  labels
- field comparison filters over original labels, structured metadata, and
  parser-extracted labels, including numeric comparisons such as `status >= 500`
  and string regex operators `=~` / `!~`
- line and label formatting with `{{.label}}`, `{{__line__}}`, and
  `{{__timestamp__ | unixEpochNanos}}` template access, including string helper
  pipelines such as `b64enc`, `b64dec`, `date`, `bytes`, `duration`,
  `duration_seconds`, `print`, `printf`, `println`, `urlquery`, `html`, `js`, `len`, `unixToTime`, `toDate`, `toDateInZone`, `now`,
  `fromJson` with `index` / `slice` helpers and `range` blocks over decoded arrays, integer math helpers,
  float math helpers, ordering comparison helpers, query/HTML/JS escaping helpers, conditional template blocks, boolean template
  combinators, and Go-template variable declarations such as
  `{{ $line := __line__ }}`
- instant metric query responses as Loki `vector` JSON and range metric query
  responses as Loki `matrix` JSON
- the first metric range aggregations:
  `count_over_time({app="api"} |= "error" [30s])`
  `present_over_time({app="api"} |= "error" [30s])`
  `rate({app="api"} |= "error" [30s])`
  `bytes_rate({app="api"} |= "error" [30s])`
  `bytes_over_time({app="api"} |= "error" [30s])`
  `absent_over_time({app="api"} [30s])`
  metric queries reject surviving pipeline errors such as `JSONParserErr`
  unless the pipeline filters them out with `__error__=""`
  `rate({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `rate_counter({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `sum_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `sum_over_time({app="api"} | logfmt | unwrap cost | __error__="" [30s])` with signed decimal and scientific-notation samples
  `sum_over_time({app="api"} | logfmt | unwrap bytes(size) | __error__="" [30s])`
  `sum_over_time({app="api"} | logfmt | unwrap duration(latency) | __error__="" [30s])`
  `avg_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `avg_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s]) by (app)`
  `stdvar_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `stddev_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `quantile_over_time(0.75, {app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `min_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `max_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `first_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
  `last_over_time({app="api"} | logfmt | unwrap value | __error__="" [30s])`
- Prometheus-style ordered compound duration literals such as `1h30m` in LogQL
  metric range selectors
- Loki range-vector offsets such as
  `count_over_time({app="api"} |= "error" [30s] offset 5m)`
- vector aggregations over those range aggregations:
  `sum` / `count` / `min` / `max` / `avg` / `stdvar` / `stddev` with
  `by(...)` and `without(...)`, parameterized `topk` / `bottomk` selection,
  Loki's `approx_topk` function using exact deterministic selection, and
  original-series ordering with `sort` / `sort_desc`
- Loki's `vector(s)` scalar-vector function with signed decimal and
  scientific-notation scalars for instant vectors and stepped range matrices,
  including `+`, `-`, `*`, `/`, `%`, and `^` arithmetic between `vector(...)`
  terms and scalar literal operands, parenthesized arithmetic groups,
  comparison operators with default filtering and `bool` values, plus no-op
  `on(...)` / `ignoring(...)` vector matching modifiers with optional
  `group_left(...)` / `group_right(...)` group modifiers and the Grafana
  health-check scalar addition expression `vector(1)+vector(1)`
- Loki's `label_replace(...)` and `label_join(...)` functions for the synthetic
  single-series vector path and metric query vectors, including `$1`-style regex
  capture expansion and separator-joined source label values into output labels;
  the shared LogQL parser and `/format_query` endpoint also accept `vector(...)`
  and metric-query wrappers
- metric query vector comparisons against scalar literals on either side,
  including default filtering and `bool` output samples
- metric query vector arithmetic with scalar literals on either side for instant
  vectors and stepped matrices
- metric query vector/vector arithmetic with exact label-set matching for
  instant vectors and stepped matrices, plus `on(...)` / `ignoring(...)` vector
  matching modifiers and `group_left(...)` / `group_right(...)` group modifiers
- metric query vector/vector comparisons with exact label-set matching, default
  filtering, `bool` output samples, `on(...)` / `ignoring(...)` vector matching
  modifiers, and `group_left(...)` / `group_right(...)` group modifiers for
  instant vectors and stepped matrices
- metric query vector/vector logical/set operators `and`, `or`, and `unless`
  with exact label-set matching plus `on(...)` / `ignoring(...)` one-to-one
  vector matching modifiers for instant vectors and stepped matrices
- instant `vector(...)` logical/set operators for the synthetic single-series
  vector path: `and`, `or`, and `unless`
- instant scalar arithmetic queries such as `1+1`, returning Loki scalar
  result JSON
- quoted string values with basic escaping, including `\n`, `\r`, `\t`, `\"`,
  and `\\` in LogQL and logfmt quoted values

That is enough for the first `query` / `query_range` streams endpoint, label
metadata endpoints, and one `query_range` metric matrix shape. Its planner now
lowers those stream selectors into `crabka-blockstore` candidate blocks. The
observability executor now feeds structured metadata into the LogQL matcher and
returned stream label sets, so persisted cold-block rows can be filtered by
parsed fields, line-body fields, and push-time structured metadata.
`count_over_time`, `present_over_time`, `rate`, `bytes_over_time`, and
`bytes_rate` now support both the single end-sample helper and stepped `query_range` matrix samples. Vector
aggregations now group and reduce stepped matrix samples, including population
`stdvar` and `stddev`, count sample-value occurrences with `count_values`,
select original input series for `topk` / `bottomk` and `approx_topk`, and
order original input series for `sort` / `sort_desc`.
Supported unwrapped range aggregations can also group raw range states with
direct trailing `by(...)` / `without(...)` clauses before reducing them to
sample values. Range-vector offsets shift cold-block and hot-tail sample windows
without changing the emitted Loki sample timestamp. The remaining
metric-query subset should stay in later slices.

### 8. Role selection can stay thin while the roles fill in

The production `crabka-observability` crate now owns the `--target
distributor|compactor|querier` binary surface. `build_service_router` now turns
that typed config plus injected runtime dependencies into distributor and
querier HTTP routers: distributor startup requires a WAL sink, while querier
startup can load local manifests, tenant object-store manifests, or the
object-store shard catalog. `build_service_dependencies` now builds configured
Kafka WAL clients for distributor, compactor, and querier roles; the querier
consumer feeds the buffered hot-tail poller that route construction attaches to
queries. `--object-store-url` now lets runtime startup build a local filesystem,
S3, or GCS `object_store` backend from config and combine the URL root with
`--index-prefix`; a local `file://` test proves the querier can bootstrap from
configured object-store state without an injected test store. `--listen-addr`,
`serve_service`, and `serve_service_listener` then bind and serve the selected
router; TCP-level tests post Loki JSON through the distributor listener and
prove the compactor listener serves status HTTP while the long-running WAL
consumer loop polls concurrently.

The distributor has its first real ingress slices: `POST /loki/api/v1/push`
accepts Loki's JSON stream payload and snappy-compressed protobuf push payload,
`POST /v1/logs` and Loki's `POST /otlp/v1/logs` accept OTLP/HTTP JSON or
protobuf logs, and the tonic `LogsService` accepts OTLP/gRPC logs. All three
paths take the tenant from
`X-Scope-OrgID`/`x-scope-orgid`, normalize entries into `WalLogRecord`, and
normalize OTLP attribute names such as `service.name` to Loki-compatible
`service_name` label / structured-metadata keys, preserve protobuf `trace_id`
and `span_id` fields as structured metadata for correlation, and populate
`service_name` from candidate labels such as `app` when it is not sent
explicitly before falling back to `unknown_service` and appending through an
async `LogWalSink`.
`KafkaLogWalSink` now wires that trait to
`crabka-client-producer`: it builds a producer from `--wal-bootstrap-server`,
uses `--wal-topic`, requests `acks=all`, keys records by `(tenant,
series_fingerprint)`, stores the full normalized record as JSON, and propagates
serialization / delivery failures back to the HTTP error path. The in-memory
test sink still proves labels, nanosecond timestamps, log lines, and optional
structured metadata survive normalization; the Kafka record test proves the
durable WAL encoding boundary, and the compactor now also accepts native
Kafka-produced log-line records with tenant, labels, timestamps, and structured
metadata in headers, rejecting malformed native label and metadata header names
and duplicate native label/metadata header names, plus invalid explicit or
broker-derived timestamps, before they enter hot-tail or compactor decode paths.
Configured distributor routes can now reject oversized HTTP ingest bodies and
injected tenant-quota decisions with Loki-style `429` / `rate_limited`
responses. Configured distributors now also install a broker-backed ingest
limiter by default: it reads the tenant's Crabka `producer_byte_rate` quota via
`DescribeClientQuotas` and applies that byte-rate check before WAL append, with
live-broker coverage proving quota rejection leaves the WAL topic empty.
Configured distributor routes can bound stalled WAL appends with
`--wal-append-timeout-ms`, returning Loki-style `503` / `server_error` instead
of hanging under WAL backpressure. OTLP gRPC can reject injected quota decisions
with `ResourceExhausted`; quota checks happen before WAL append. Loki push
validation now covers malformed stream label names, duplicate protobuf stream
labels, structured metadata names, and the JSON push requirement that structured
metadata values be flat strings before the WAL boundary. Real-Loki differential
coverage now includes the malformed JSON stream-label path, the non-string JSON
structured-metadata value path, and the duplicate JSON stream-label path where
Loki accepts the later value and returns `204`. The corpus also pins empty JSON
stream labels: Loki accepts `{}` and falls through service-name discovery to
`unknown_service`, so Crabka now appends those records instead of rejecting them
as empty labels. Configured distributor services now also enforce Loki's
default one-week stale-sample rejection for Loki push payloads, returning the
same plain-text `400` timestamp-too-old response before WAL append; low-level
in-memory router tests leave that gate disabled so deterministic fixture
timestamps can continue to exercise query behavior. Configured distributors also
enforce Loki's default ten-minute creation grace for future Loki push samples,
returning the same plain-text `400` timestamp-too-new response before WAL
append, with real-Loki differential coverage pinning the response body.
Malformed JSON push entry fields now also match Loki's raw
`loghttp.PushRequest.Streams` unmarshaler text responses instead of Crabka's
generic Loki error envelope, covering timestamp strings that fail numeric
parsing, non-string log-line values, and non-object structured metadata fields.
Timestamp-only JSON push values now match Loki by appending an empty log line
instead of rejecting the value shape, and extra JSON push value fields after the
first structured metadata object are ignored like Loki. Non-array JSON push
values now also return Loki's raw `Unknown value type` decoder response, and
non-array `streams` fields and non-object JSON stream entries return Loki's raw
stream decoder responses. Missing or empty top-level JSON `streams` now return
Loki's `422` no-valid-streams text response, while stream objects with omitted
or `null` `values` fields or omitted `stream` labels are accepted as no-op `204`
pushes like Loki. Stream objects with non-array, non-null `values` fields or
non-object `stream` fields, including `stream: null`, now return Loki's raw
decoder text responses instead of Crabka's generic invalid-payload envelope.
Top-level JSON array payloads now return Loki's `readObjectStart` decoder text,
and top-level `null` payloads return Loki's `422` no-valid-streams text. The
configured OTLP HTTP ingest path now shares the same timestamp windows before
WAL append and matches Loki's `400` protobuf status-message body for future
OTLP timestamps. Configured distributors now also perform a broker-backed tenant
write-ACL check before WAL append: when any ACLs exist, `User:<X-Scope-OrgID>`
must have `Write` or `All` permission on the WAL topic, deny entries win, and
no-ACL clusters retain the broker's compatibility allow behavior. Configured
queriers mirror that ACL policy for reads: `User:<X-Scope-OrgID>` must have
`Read` or `All` permission on the WAL topic before any tenant labels, blocks,
or hot-tail records are queried. Live-broker coverage proves the distributor
missing-ACL `403` rejection path, the allowed-tenant append path, and the
configured-querier missing read-ACL `403` path. Remaining distributor work is
deeper Loki validation parity, WAL backpressure exercises against live broker
degradation, and broader distributor differential-vs-Loki coverage.

### 9. Arrow version alignment is the concrete dependency risk

The first run failed after a successful compile because the example wrote with
workspace Arrow/Parquet 59 but DataFusion returned Arrow 58 arrays. The fix was
to use `datafusion::arrow` and `datafusion::parquet` re-exports in the spike
example.

Production recommendation:

- Avoid exposing raw Arrow array concrete types across Crabka crate boundaries
  unless all query crates agree on the same Arrow version.
- Put the DataFusion-facing block reader/table provider in the same crate that
  depends on DataFusion, and use DataFusion's re-exported Arrow types there.
- Keep generic block metadata and index types Arrow-free where possible.

### 10. DataFusion compile cost is real

The first example build took about 3 minutes 23 seconds on this worktree after
adding DataFusion 54 and its dependency stack. Subsequent example builds were a
few seconds. This is acceptable for the observability/query crates, but argues
against making DataFusion a dependency of broker hot-path crates.

## Recommended plan changes

1. Make `crabka-blockstore` split into an Arrow-free metadata/index layer plus a
   DataFusion-enabled table-provider module.
2. Use DataFusion re-exported Arrow/Parquet types anywhere DataFusion batches
   are produced or consumed.
3. Use Crabka-owned xxh3 stream fingerprints for the MVP; document that Loki
   fingerprint parity is deferred and unnecessary for Grafana API compatibility.
4. Next production slices should attach persistence and serving to the current
   parser/planner/index core:
   - extend `LogBlockTableProvider` route integration to use object-store scans
     and participate in the hot/cold union plan
   - broaden live broker coverage beyond the current direct WAL, config-built
     startup, push → compact → query, hot/cold-frontier, and compactor-restart
     tests into harder crash-in-the-middle scenarios
   - endpoint-by-endpoint Loki error status/message parity beyond the current
     shared error envelope and the current query scalar-param parser
5. Keep DataFusion out of the broker crate. The observability service should
   remain a separate role-selectable process as the design already says.

## Spike artifacts

- Production blockstore slice: `crates/blockstore/src/lib.rs`,
  `crates/blockstore/tests/index.rs`, `crates/blockstore/tests/parquet.rs`,
  `crates/blockstore/tests/datafusion.rs`
- Production LogQL parser/planner slice: `crates/logql/src/lib.rs`,
  `crates/logql/tests/parser.rs`, `crates/logql/tests/planner.rs`
- Production service slice: `crates/observability/src/lib.rs`,
  `crates/observability/src/main.rs`, `crates/observability/tests/cli.rs`,
  `crates/observability/tests/querier.rs`,
  `crates/observability/tests/http.rs`
- Plan: `docs/superpowers/plans/2026-06-18-loki-spike.md`
- Core/tests: `crates/observability-spike/src/lib.rs`,
  `crates/observability-spike/tests/core.rs`
- Example: `crates/observability-spike/examples/loki_spike.rs`

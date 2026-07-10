//! Legacy `POST /ingest` door.

use std::{collections::BTreeMap, io::Cursor};

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;
use serde::Deserialize;

use crate::{error::ProfilesError, ingest::RawProfile};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFormat {
    Pprof,
    Jfr,
    Trie,
    Tree,
    Lines,
    Speedscope,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQuery {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub format: IngestFormat,
    pub sample_rate: u32,
    pub units: String,
    pub from_ms: Option<i64>,
    pub until_ms: Option<i64>,
}

pub fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError> {
    let mut name = String::new();
    let mut labels = Vec::new();
    let mut format = IngestFormat::Groups;
    let mut sample_rate = 100;
    let mut units = "count".to_string();
    let mut from_ms = None;
    let mut until_ms = None;

    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = urldecode(value);
        match key {
            "name" => {
                (name, labels) = split_app_labels(&value)?;
            }
            "format" => {
                format = match value.as_str() {
                    "pprof" => IngestFormat::Pprof,
                    "jfr" => IngestFormat::Jfr,
                    "trie" => IngestFormat::Trie,
                    "tree" => IngestFormat::Tree,
                    "lines" => IngestFormat::Lines,
                    "speedscope" => IngestFormat::Speedscope,
                    _ => IngestFormat::Groups,
                };
            }
            "sampleRate" => {
                sample_rate = value.parse().unwrap_or(100);
            }
            "units" => {
                if !value.is_empty() {
                    units = value;
                }
            }
            "from" => {
                from_ms = Some(parse_unix_time_ms(&value)?);
            }
            "until" => {
                until_ms = Some(parse_unix_time_ms(&value)?);
            }
            _ => {}
        }
    }

    if name.is_empty() {
        return Err(ProfilesError::Invalid("missing ?name".to_string()));
    }

    Ok(IngestQuery {
        name,
        labels,
        format,
        sample_rate,
        units,
        from_ms,
        until_ms,
    })
}

pub async fn decode_ingest_multipart(
    query: &IngestQuery,
    content_type: &str,
    body: bytes::Bytes,
    max: usize,
) -> Result<RawProfile, ProfilesError> {
    let boundary =
        multer::parse_boundary(content_type).map_err(|e| ProfilesError::Invalid(e.to_string()))?;
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut pprof_bytes = None;
    let mut folded_bytes = None;
    let mut jfr_bytes = None;
    let mut multipart_labels = Vec::new();
    let mut sample_type_config = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ProfilesError::Invalid(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| ProfilesError::Invalid(e.to_string()))?;
        if data.len() > max {
            return Err(ProfilesError::TooLarge { limit: max });
        }
        match name.as_str() {
            "profile" if query.format == IngestFormat::Pprof => pprof_bytes = Some(data.to_vec()),
            "sample_type_config" if query.format == IngestFormat::Pprof => {
                sample_type_config = Some(parse_sample_type_config(&data)?);
            }
            "profile" | "groups" | "folded"
                if matches!(query.format, IngestFormat::Groups | IngestFormat::Lines) =>
            {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "tree" if query.format == IngestFormat::Tree => {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "trie" if query.format == IngestFormat::Trie => {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "speedscope" if query.format == IngestFormat::Speedscope => {
                folded_bytes = Some(data.to_vec());
            }
            "jfr" if query.format == IngestFormat::Jfr => jfr_bytes = Some(data.to_vec()),
            "labels" if query.format == IngestFormat::Jfr => {
                multipart_labels = parse_labels_part(&data)?;
            }
            _ => {}
        }
    }

    let delta = sample_type_config
        .as_ref()
        .and_then(|config| config.cumulative)
        .is_some_and(|cumulative| !cumulative);
    let profile = match query.format {
        IngestFormat::Pprof => {
            let raw = pprof_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart `profile` part".to_string())
            })?;
            let profile = PprofProfile::decode(&raw)?;
            if let Some(config) = &sample_type_config {
                apply_sample_type_config(profile, config)
            } else {
                profile
            }
        }
        IngestFormat::Groups => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart folded `profile` part".to_string())
            })?;
            folded_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&raw))?
        }
        IngestFormat::Lines => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart lines `profile` part".to_string())
            })?;
            lines_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&raw))?
        }
        IngestFormat::Tree => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart tree `profile` part".to_string())
            })?;
            tree_to_pprof(&query.name, &query.units, &raw)?
        }
        IngestFormat::Trie => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart trie `profile` part".to_string())
            })?;
            trie_to_pprof(&query.name, &query.units, &raw)?
        }
        IngestFormat::Speedscope => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart speedscope `profile` part".to_string())
            })?;
            speedscope_to_pprof(&query.name, &query.units, &raw)?
        }
        IngestFormat::Jfr => {
            let raw = jfr_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart `jfr` part".to_string())
            })?;
            jfr_to_pprof(&query.name, &raw)?
        }
    };
    let profile = apply_query_time(profile, query)?;

    Ok(RawProfile {
        labels: query_labels(query, multipart_labels),
        profile,
        delta,
        sample_timestamps_ns: Vec::new(),
        sample_span_ids: Vec::new(),
        sample_trace_ids: Vec::new(),
    })
}

pub async fn decode_ingest_body(
    query: &IngestQuery,
    content_type: Option<&str>,
    body: bytes::Bytes,
    max: usize,
) -> Result<RawProfile, ProfilesError> {
    if let Some(content_type) = content_type
        && content_type
            .split(';')
            .next()
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"))
    {
        return decode_ingest_multipart(query, content_type, body, max).await;
    }

    if body.len() > max {
        return Err(ProfilesError::TooLarge { limit: max });
    }

    let profile = match query.format {
        IngestFormat::Groups => {
            folded_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?
        }
        IngestFormat::Lines => {
            lines_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?
        }
        IngestFormat::Trie => trie_to_pprof(&query.name, &query.units, &body)?,
        IngestFormat::Tree => tree_to_pprof(&query.name, &query.units, &body)?,
        IngestFormat::Speedscope => speedscope_to_pprof(&query.name, &query.units, &body)?,
        IngestFormat::Pprof => {
            return Err(ProfilesError::Invalid(
                "legacy pprof ingest requires multipart `profile` part".to_string(),
            ));
        }
        IngestFormat::Jfr => {
            return Err(ProfilesError::Invalid(
                "legacy jfr ingest requires multipart `jfr` part".to_string(),
            ));
        }
    };
    let profile = apply_query_time(profile, query)?;
    Ok(RawProfile {
        labels: query_labels(query, Vec::new()),
        profile,
        delta: false,
        sample_timestamps_ns: Vec::new(),
        sample_span_ids: Vec::new(),
        sample_trace_ids: Vec::new(),
    })
}

fn parse_unix_time_ms(value: &str) -> Result<i64, ProfilesError> {
    let value = value.trim();
    let numeric = value
        .parse::<i64>()
        .map_err(|err| ProfilesError::Invalid(format!("invalid ingest time {value:?}: {err}")))?;
    Ok(if numeric.abs() < 10_000_000_000 {
        numeric.saturating_mul(1000)
    } else {
        numeric
    })
}

fn apply_query_time(
    profile: PprofProfile,
    query: &IngestQuery,
) -> Result<PprofProfile, ProfilesError> {
    if profile.inner().time_nanos != 0 {
        return Ok(profile);
    }
    let Some(timestamp_ms) = query.until_ms.or(query.from_ms) else {
        return Ok(profile);
    };
    let time_nanos = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        ProfilesError::Invalid(format!(
            "ingest timestamp overflows nanoseconds: {timestamp_ms}"
        ))
    })?;
    let mut profile = profile.into_inner();
    profile.time_nanos = time_nanos;
    Ok(PprofProfile::from(profile))
}

fn query_labels(query: &IngestQuery, extra_labels: Vec<(String, String)>) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", query.name.clone());
    for (name, value) in &query.labels {
        labels.insert(name.clone(), value.clone());
    }
    for (name, value) in extra_labels {
        labels.insert(name, value);
    }
    labels
}

#[derive(Debug, Deserialize)]
struct SampleTypeConfig {
    units: Option<String>,
    #[serde(rename = "display-name")]
    display_name: Option<String>,
    #[allow(dead_code)]
    aggregation: Option<String>,
    cumulative: Option<bool>,
    #[allow(dead_code)]
    sampled: Option<bool>,
}

fn parse_sample_type_config(raw: &[u8]) -> Result<SampleTypeConfig, ProfilesError> {
    serde_json::from_slice(raw)
        .map_err(|err| ProfilesError::Decode(format!("sample_type_config is not JSON: {err}")))
}

fn apply_sample_type_config(profile: PprofProfile, config: &SampleTypeConfig) -> PprofProfile {
    let mut profile = profile.into_inner();
    let Some(sample_type) = profile.sample_type.first().copied() else {
        return PprofProfile::from(profile);
    };
    let sample_type_name = config
        .display_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(sample_type.r#type, |value| {
            intern_profile_string(&mut profile.string_table, value)
        });
    let sample_unit = config
        .units
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(sample_type.unit, |value| {
            intern_profile_string(&mut profile.string_table, value)
        });
    if let Some(first) = profile.sample_type.first_mut() {
        first.r#type = sample_type_name;
        first.unit = sample_unit;
    }
    profile.period_type = Some(crabka_pprof::proto::ValueType {
        r#type: sample_type_name,
        unit: sample_unit,
    });
    profile.default_sample_type = sample_type_name;
    PprofProfile::from(profile)
}

fn intern_profile_string(strings: &mut Vec<String>, value: &str) -> i64 {
    if let Some(idx) = strings.iter().position(|existing| existing == value) {
        return i64::try_from(idx).expect("string index fits i64");
    }
    let idx = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    idx
}

fn jfr_to_pprof(name: &str, raw: &[u8]) -> Result<PprofProfile, ProfilesError> {
    if raw.starts_with(b"FLR\0") {
        return binary_jfr_to_pprof(name, raw);
    }
    let body = std::str::from_utf8(raw).map_err(|err| {
        ProfilesError::Decode(format!("jfr payload is not UTF-8 collapsed stacks: {err}"))
    })?;
    folded_to_pprof(name, "count", body)
}

fn binary_jfr_to_pprof(name: &str, raw: &[u8]) -> Result<PprofProfile, ProfilesError> {
    let mut reader = jfrs::reader::JfrReader::new(Cursor::new(raw.to_vec()));
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for chunk in reader.chunks() {
        let (mut chunk_reader, chunk) =
            chunk.map_err(|err| ProfilesError::Decode(format!("jfr chunk decode: {err}")))?;
        for event in chunk_reader.events(&chunk) {
            let event =
                event.map_err(|err| ProfilesError::Decode(format!("jfr event decode: {err}")))?;
            if event.class.name() != "jdk.ExecutionSample" {
                continue;
            }
            let sample: jfrs::reader::types::jdk::ExecutionSample<'_> =
                jfrs::reader::from_event(&event).map_err(|err| {
                    ProfilesError::Decode(format!("jfr execution sample decode: {err}"))
                })?;
            let Some(stack) = sample.stack_trace else {
                continue;
            };
            let frames = stack
                .frames
                .into_iter()
                .flatten()
                .filter_map(|frame| {
                    let method = frame.method?;
                    let method_name = method.name.and_then(|name| name.string)?;
                    Some((
                        jfr_method_name(method.class, method_name),
                        frame.line_number,
                    ))
                })
                .collect::<Vec<_>>();
            if !frames.is_empty() {
                *stacks.entry(frames).or_default() += 1;
            }
        }
    }
    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "jfr profile has no execution samples".to_string(),
        ));
    }
    stacks_to_pprof(name, "wall", "nanoseconds", stacks)
}

fn jfr_method_name(
    class: Option<jfrs::reader::types::builtin::Class<'_>>,
    method_name: &str,
) -> String {
    if method_name.contains("::") {
        return method_name.to_string();
    }
    class
        .and_then(|class| class.name)
        .and_then(|name| name.string)
        .map_or_else(
            || method_name.to_string(),
            |class_name| format!("{}.{}", class_name.replace('/', "."), method_name),
        )
}

fn parse_labels_part(raw: &[u8]) -> Result<Vec<(String, String)>, ProfilesError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_slice(raw)
        .map_err(|err| ProfilesError::Decode(format!("jfr labels part is not JSON: {err}")))?;
    let object = json.as_object().ok_or_else(|| {
        ProfilesError::Decode("jfr labels part must be a JSON object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Number(value) => value.to_string(),
                serde_json::Value::Bool(value) => value.to_string(),
                serde_json::Value::Null => String::new(),
                _ => {
                    return Err(ProfilesError::Decode(format!(
                        "jfr label `{key}` must be a scalar"
                    )));
                }
            };
            Ok((key.clone(), value))
        })
        .collect()
}

fn folded_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &str,
) -> Result<PprofProfile, ProfilesError> {
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for (line_no, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (stack, value) = line.rsplit_once(char::is_whitespace).ok_or_else(|| {
            ProfilesError::Decode(format!("folded line {} missing value", line_no + 1))
        })?;
        let value = value.parse::<i64>().map_err(|err| {
            ProfilesError::Decode(format!(
                "folded line {} has invalid value: {err}",
                line_no + 1
            ))
        })?;
        let frames = stack
            .split(';')
            .filter(|frame| !frame.is_empty())
            .map(|frame| (frame.to_string(), 0))
            .collect::<Vec<_>>();
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "folded line {} has empty stack",
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += value;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "folded profile has no samples".to_string(),
        ));
    }
    stacks_to_pprof(name, "samples", sample_unit, stacks)
}

fn lines_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &str,
) -> Result<PprofProfile, ProfilesError> {
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for (line_no, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let frames = line
            .split(';')
            .filter(|frame| !frame.is_empty())
            .map(|frame| (frame.to_string(), 0))
            .collect::<Vec<_>>();
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "lines profile line {} has empty stack",
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += 1;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "lines profile has no samples".to_string(),
        ));
    }
    stacks_to_pprof(name, "samples", sample_unit, stacks)
}

/// Absolute cap on the number of tree/trie nodes a single payload may expand
/// into. A malformed or hostile payload can declare child counts that amplify
/// far beyond the input byte length (each pending entry carries a cloned path),
/// so we bound the total work independently of `body.len()`.
const MAX_TREE_NODES: usize = 500_000;

/// Absolute cap on the cumulative bytes of all materialized stack paths/keys.
/// Path bytes are copied per child during expansion, so a deep-and-wide payload
/// can amplify path storage far past the input size even when node count is
/// under control.
const MAX_TREE_PATH_BYTES: usize = 64 * 1024 * 1024;

/// Hard cap on trie recursion depth (modeled as an explicit work-stack). A
/// deeply nested payload would otherwise overflow the native stack.
const MAX_TRIE_DEPTH: usize = 4_096;

fn tree_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &[u8],
) -> Result<PprofProfile, ProfilesError> {
    let mut pos = 0;
    let mut pending = vec![Vec::<(String, i32)>::new()];
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut node_count = 0_usize;
    let mut path_bytes = 0_usize;

    while let Some(parent_path) = pending.pop() {
        node_count += 1;
        if node_count > MAX_TREE_NODES {
            return Err(ProfilesError::Decode(
                "tree profile exceeds node budget".to_string(),
            ));
        }

        let name_len = read_tree_varint(body, &mut pos, "node name length")?;
        let name_len = usize::try_from(name_len).map_err(|err| {
            ProfilesError::Decode(format!("tree node name length does not fit usize: {err}"))
        })?;
        let name_end = pos.checked_add(name_len).ok_or_else(|| {
            ProfilesError::Decode("tree node name length overflows payload offset".to_string())
        })?;
        if name_end > body.len() {
            return Err(ProfilesError::Decode(
                "tree node name length exceeds payload".to_string(),
            ));
        }
        let name = std::str::from_utf8(&body[pos..name_end])
            .map_err(|err| ProfilesError::Decode(format!("tree node name is not UTF-8: {err}")))?;
        pos = name_end;

        let self_value = read_tree_varint(body, &mut pos, "node self value")?;
        let children_len = read_tree_varint(body, &mut pos, "node children length")?;
        let children_len = usize::try_from(children_len).map_err(|err| {
            ProfilesError::Decode(format!(
                "tree node children length does not fit usize: {err}"
            ))
        })?;
        if children_len > body.len().saturating_sub(pos) + 1 {
            return Err(ProfilesError::Decode(
                "tree node children length exceeds remaining payload".to_string(),
            ));
        }

        let mut path = parent_path;
        if !name.is_empty() {
            path.push((name.to_string(), 0));
        }
        if self_value > 0 && !path.is_empty() {
            let value = i64::try_from(self_value).map_err(|err| {
                ProfilesError::Decode(format!("tree node self value does not fit i64: {err}"))
            })?;
            *stacks.entry(path.clone()).or_default() += value;
        }

        if children_len > 0 {
            // Each child gets its own clone of `path`; charge that copied
            // storage against the cumulative path-bytes budget so a payload
            // declaring many children of a long path cannot amplify memory
            // beyond the cap.
            let per_child_bytes = path
                .iter()
                .map(|(frame, _)| frame.len())
                .fold(0_usize, usize::saturating_add);
            let added = per_child_bytes.saturating_mul(children_len);
            path_bytes = path_bytes.saturating_add(added);
            if path_bytes > MAX_TREE_PATH_BYTES {
                return Err(ProfilesError::Decode(
                    "tree profile exceeds path-bytes budget".to_string(),
                ));
            }
            // Also bound the queued node count up front so an enormous declared
            // child count cannot balloon `pending` before the per-iteration
            // `node_count` check trips.
            if node_count
                .saturating_add(pending.len())
                .saturating_add(children_len)
                > MAX_TREE_NODES
            {
                return Err(ProfilesError::Decode(
                    "tree profile exceeds node budget".to_string(),
                ));
            }
            pending.extend(std::iter::repeat_n(path, children_len));
        }
    }

    if pos != body.len() {
        return Err(ProfilesError::Decode(
            "tree profile has trailing bytes".to_string(),
        ));
    }
    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "tree profile has no samples".to_string(),
        ));
    }
    stacks_to_pprof(name, "samples", sample_unit, stacks)
}

fn read_tree_varint(body: &[u8], pos: &mut usize, field: &str) -> Result<u64, ProfilesError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *body
            .get(*pos)
            .ok_or_else(|| ProfilesError::Decode(format!("tree payload ended before {field}")))?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(ProfilesError::Decode(format!(
                "tree {field} varint overflows u64"
            )));
        }
    }
}

fn trie_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &[u8],
) -> Result<PprofProfile, ProfilesError> {
    let mut pos = 0;
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut node_count = 0_usize;
    let mut path_bytes = 0_usize;

    // Explicit work-stack: each frame is a node prefix paired with the number
    // of sibling children that still need to be visited at this depth. The
    // stack length is the live recursion depth, hard-capped below so a deeply
    // nested payload cannot overflow the native stack.
    //
    // The top-level forest is modeled as a synthetic root whose "remaining"
    // count is consumed one node per outer step until the payload is exhausted.
    let mut work: Vec<TrieFrame> = Vec::new();

    loop {
        // Unwind any completed parents first so `work.len()` reflects true live
        // depth and the exhaustion check below isn't tripped by spent frames.
        while let Some(frame) = work.last() {
            if frame.remaining == 0 {
                work.pop();
            } else {
                break;
            }
        }

        if pos >= body.len() {
            if work.is_empty() {
                break;
            }
            // Out of bytes but the work-stack still expects children: malformed.
            return Err(ProfilesError::Decode(
                "trie payload ended before all declared children".to_string(),
            ));
        }

        if work.len() >= MAX_TRIE_DEPTH {
            return Err(ProfilesError::Decode(
                "trie profile exceeds maximum depth".to_string(),
            ));
        }

        node_count += 1;
        if node_count > MAX_TREE_NODES {
            return Err(ProfilesError::Decode(
                "trie profile exceeds node budget".to_string(),
            ));
        }

        let prefix: &[u8] = work.last().map_or(&[][..], |frame| frame.key.as_slice());

        let suffix_len = read_tree_varint(body, &mut pos, "trie node suffix length")?;
        let suffix_len = usize::try_from(suffix_len).map_err(|err| {
            ProfilesError::Decode(format!("trie node suffix length does not fit usize: {err}"))
        })?;
        let suffix_end = pos.checked_add(suffix_len).ok_or_else(|| {
            ProfilesError::Decode("trie node suffix length overflows payload offset".to_string())
        })?;
        if suffix_end > body.len() {
            return Err(ProfilesError::Decode(
                "trie node suffix length exceeds payload".to_string(),
            ));
        }

        let mut key = Vec::with_capacity(prefix.len().saturating_add(suffix_len));
        key.extend_from_slice(prefix);
        key.extend_from_slice(&body[pos..suffix_end]);
        pos = suffix_end;

        // Charge the materialized key length against the cumulative budget.
        // Long shared prefixes are copied into every descendant, so a hostile
        // payload can amplify key storage well beyond the input size.
        path_bytes = path_bytes.saturating_add(key.len());
        if path_bytes > MAX_TREE_PATH_BYTES {
            return Err(ProfilesError::Decode(
                "trie profile exceeds path-bytes budget".to_string(),
            ));
        }

        let value = read_tree_varint(body, &mut pos, "trie node value")?;
        let children_len = read_tree_varint(body, &mut pos, "trie node children length")?;
        let children_len = usize::try_from(children_len).map_err(|err| {
            ProfilesError::Decode(format!(
                "trie node children length does not fit usize: {err}"
            ))
        })?;
        if children_len > body.len().saturating_sub(pos) + 1 {
            return Err(ProfilesError::Decode(
                "trie node children length exceeds remaining payload".to_string(),
            ));
        }

        if value > 0 {
            let value = i64::try_from(value).map_err(|err| {
                ProfilesError::Decode(format!("trie node value does not fit i64: {err}"))
            })?;
            let key_str = std::str::from_utf8(&key)
                .map_err(|err| ProfilesError::Decode(format!("trie key is not UTF-8: {err}")))?;
            let frames = key_str
                .split(';')
                .filter(|frame| !frame.is_empty())
                .map(|frame| (frame.to_string(), 0))
                .collect::<Vec<_>>();
            if frames.is_empty() {
                return Err(ProfilesError::Decode(
                    "trie profile has an empty stack".to_string(),
                ));
            }
            *stacks.entry(frames).or_default() += value;
        }

        // This node consumed one of its parent's remaining child slots.
        if let Some(frame) = work.last_mut() {
            frame.remaining -= 1;
        }
        // Descend if this node declares children.
        if children_len > 0 {
            work.push(TrieFrame {
                key,
                remaining: children_len,
            });
        }
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "trie profile has no samples".to_string(),
        ));
    }
    stacks_to_pprof(name, "samples", sample_unit, stacks)
}

/// One level of the explicit trie work-stack: the accumulated key prefix for a
/// node plus how many of its declared children remain to be visited.
struct TrieFrame {
    key: Vec<u8>,
    remaining: usize,
}

fn speedscope_to_pprof(
    name: &str,
    default_unit: &str,
    body: &[u8],
) -> Result<PprofProfile, ProfilesError> {
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| ProfilesError::Decode(format!("speedscope profile is not JSON: {err}")))?;
    let frames = json
        .pointer("/shared/frames")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ProfilesError::Decode("speedscope shared.frames missing".to_string()))?;
    let frame_names = frames
        .iter()
        .enumerate()
        .map(|(idx, frame)| {
            frame
                .get("name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    ProfilesError::Decode(format!("speedscope shared.frames[{idx}].name missing"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let profiles = json
        .get("profiles")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ProfilesError::Decode("speedscope profiles missing".to_string()))?;
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut sample_unit = default_unit.to_string();

    for (profile_idx, profile) in profiles.iter().enumerate() {
        let profile_type = profile
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if profile_type != "sampled" {
            continue;
        }
        if let Some(unit) = profile
            .get("unit")
            .and_then(serde_json::Value::as_str)
            .filter(|unit| !unit.is_empty())
        {
            sample_unit = unit.to_string();
        }
        let samples = profile
            .get("samples")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples missing"
                ))
            })?;
        let weights = profile
            .get("weights")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (sample_idx, sample) in samples.iter().enumerate() {
            let stack = sample.as_array().ok_or_else(|| {
                ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples[{sample_idx}] must be an array"
                ))
            })?;
            let mut frames = Vec::new();
            for frame in stack {
                let frame_idx = frame.as_u64().ok_or_else(|| {
                    ProfilesError::Decode(format!(
                        "speedscope profiles[{profile_idx}].samples[{sample_idx}] frame index must be unsigned"
                    ))
                })?;
                let name = frame_names
                    .get(usize::try_from(frame_idx).map_err(|err| {
                        ProfilesError::Decode(format!(
                            "speedscope frame index does not fit usize: {err}"
                        ))
                    })?)
                    .ok_or_else(|| {
                        ProfilesError::Decode(format!(
                            "speedscope frame index {frame_idx} is out of bounds"
                        ))
                    })?;
                frames.push((name.clone(), 0));
            }
            if frames.is_empty() {
                return Err(ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples[{sample_idx}] has empty stack"
                )));
            }
            let value = weights
                .get(sample_idx)
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(1);
            *stacks.entry(frames).or_default() += value;
        }
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "speedscope profile has no sampled stacks".to_string(),
        ));
    }
    stacks_to_pprof(name, "samples", &sample_unit, stacks)
}

fn stacks_to_pprof(
    name: &str,
    sample_type: &str,
    sample_unit: &str,
    stacks: BTreeMap<Vec<(String, i32)>, i64>,
) -> Result<PprofProfile, ProfilesError> {
    let mut string_ids = BTreeMap::from([
        (String::new(), 0_i64),
        (sample_type.to_string(), 1_i64),
        (sample_unit.to_string(), 2_i64),
    ]);
    let mut strings = vec![
        String::new(),
        sample_type.to_string(),
        sample_unit.to_string(),
    ];
    let mut function_ids = BTreeMap::new();
    let mut functions = Vec::new();
    let mut locations = Vec::new();
    let mut samples = Vec::new();

    for (stack, value) in stacks {
        let mut location_ids = Vec::new();
        for (frame, line) in stack.into_iter().rev() {
            let function_id = if let Some(id) = function_ids.get(&frame) {
                *id
            } else {
                let name_ref = intern_string(&mut strings, &mut string_ids, &frame);
                let id = i64::try_from(functions.len() + 1).expect("function id fits i64");
                functions.push(crabka_pprof::proto::Function {
                    id: u64::try_from(id).expect("positive id fits u64"),
                    name: name_ref,
                    system_name: name_ref,
                    filename: 0,
                    start_line: 0,
                });
                locations.push(crabka_pprof::proto::Location {
                    id: u64::try_from(id).expect("positive id fits u64"),
                    line: vec![crabka_pprof::proto::Line {
                        function_id: u64::try_from(id).expect("positive id fits u64"),
                        line: i64::from(line),
                        column: 0,
                    }],
                    ..Default::default()
                });
                function_ids.insert(frame, id);
                id
            };
            location_ids.push(u64::try_from(function_id).expect("positive id fits u64"));
        }
        samples.push(crabka_pprof::proto::Sample {
            location_id: location_ids,
            value: vec![value],
            label: Vec::new(),
        });
    }

    let _ = intern_string(&mut strings, &mut string_ids, name);
    Ok(PprofProfile::from(crabka_pprof::proto::Profile {
        sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
        sample: samples,
        location: locations,
        function: functions,
        string_table: strings,
        period_type: Some(crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
        ..Default::default()
    }))
}

fn intern_string(strings: &mut Vec<String>, ids: &mut BTreeMap<String, i64>, value: &str) -> i64 {
    if let Some(id) = ids.get(value) {
        return *id;
    }
    let id = i64::try_from(strings.len()).expect("string table index fits i64");
    strings.push(value.to_string());
    ids.insert(value.to_string(), id);
    id
}

fn split_app_labels(s: &str) -> Result<(String, Vec<(String, String)>), ProfilesError> {
    let Some(open) = s.find('{') else {
        return Ok((s.to_string(), Vec::new()));
    };
    let name = s[..open].to_string();
    let inner = s[open + 1..]
        .strip_suffix('}')
        .ok_or_else(|| ProfilesError::Invalid("unterminated label set".to_string()))?;
    let mut labels = Vec::new();
    for pair in inner.split(',').filter(|part| !part.is_empty()) {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| ProfilesError::Invalid("bad label pair".to_string()))?;
        labels.push((
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        ));
    }
    Ok((name, labels))
}

fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => out.push(' '),
            b'%' => {
                let (Some(hi), Some(lo)) = (bytes.next(), bytes.next()) else {
                    out.push('%');
                    continue;
                };
                let hex = [hi, lo];
                if let Ok(hex) = std::str::from_utf8(&hex)
                    && let Ok(decoded) = u8::from_str_radix(hex, 16)
                {
                    out.push(char::from(decoded));
                    continue;
                }
                out.push('%');
                out.push(char::from(hi));
                out.push(char::from(lo));
            }
            _ => out.push(char::from(byte)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn parse_query_extracts_name_labels_format() {
        let q =
            parse_ingest_query("name=myapp{env=\"prod\",team=\"core\"}&format=pprof&sampleRate=97")
                .unwrap();

        check!(
            q == IngestQuery {
                name: "myapp".to_string(),
                labels: vec![
                    ("env".to_string(), "prod".to_string()),
                    ("team".to_string(), "core".to_string()),
                ],
                format: IngestFormat::Pprof,
                sample_rate: 97,
                units: "count".to_string(),
                from_ms: None,
                until_ms: None,
            }
        );
    }

    #[test]
    fn unknown_format_defaults_to_groups() {
        let q = parse_ingest_query("name=app").unwrap();

        assert!(matches!(q.format, IngestFormat::Groups));
    }

    #[tokio::test]
    async fn decode_multipart_pprof_profile_part() {
        let query = parse_ingest_query("name=myapp{env=\"prod\"}&format=pprof").unwrap();
        let boundary = "test-boundary";
        let pprof = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&pprof);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        check!(
            (
                raw.labels.get("__name__"),
                raw.labels.get("env"),
                raw.profile.sample_types()[0].0.as_str(),
            ) == (Some("myapp"), Some("prod"), "cpu")
        );
    }

    #[tokio::test]
    async fn decode_multipart_pprof_applies_sample_type_config() {
        let query = parse_ingest_query("name=myapp&format=pprof").unwrap();
        let boundary = "test-boundary";
        let pprof = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let config = r#"{"units":"nanoseconds","display-name":"wall","aggregation":"sum","cumulative":false,"sampled":true}"#;
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"sample_type_config\"\r\n");
        body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
        body.extend_from_slice(config.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&pprof);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        assert_eq!(
            &raw.profile.sample_types()[0],
            &("wall".to_string(), "nanoseconds".to_string())
        );
        assert_eq!(
            raw.profile.period_type_strings(),
            ("wall".to_string(), "nanoseconds".to_string())
        );
        let split = crate::ingest::split_sample_types(&raw).unwrap();
        assert!(split[0].profile_type == "myapp:wall:nanoseconds:wall:nanoseconds:delta");
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_profile_part() {
        let query = parse_ingest_query("name=myapp{env=\"prod\"}").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\nmain;idle 3\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        check!(
            (
                raw.labels.get("__name__"),
                &raw.profile.sample_types()[0],
                raw.profile.samples().len(),
            ) == (
                Some("myapp"),
                &("samples".to_string(), "count".to_string()),
                2,
            )
        );
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_applies_query_units() {
        let query = parse_ingest_query("name=myapp&units=bytes").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "bytes".to_string()));
    }

    #[tokio::test]
    async fn decode_plain_lines_counts_repeated_stack_lines() {
        let query = parse_ingest_query("name=myapp&format=lines&units=samples").unwrap();
        let body = "main;work\nmain;work\nmain;idle\nmain;work\n";

        let raw = decode_ingest_body(
            &query,
            Some("text/plain"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        assert!(values == vec![1, 3]);
    }

    #[tokio::test]
    async fn decode_plain_speedscope_sampled_profile_uses_shared_frames_and_weights() {
        let query = parse_ingest_query("name=myapp&format=speedscope&units=samples").unwrap();
        let body = r#"{
          "$schema": "https://www.speedscope.app/file-format-schema.json",
          "shared": {
            "frames": [
              { "name": "main" },
              { "name": "work" },
              { "name": "idle" }
            ]
          },
          "profiles": [{
            "type": "sampled",
            "name": "cpu",
            "unit": "samples",
            "startValue": 0,
            "endValue": 10,
            "samples": [[0, 1], [0, 1], [0, 2]],
            "weights": [2, 3, 4]
          }]
        }"#;

        let raw = decode_ingest_body(
            &query,
            Some("application/json"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        assert!(values == vec![4, 5]);
    }

    #[tokio::test]
    async fn decode_plain_tree_format_payload_uses_serialized_tree_nodes() {
        let query = parse_ingest_query("name=myapp&format=tree&units=samples").unwrap();
        let body =
            bytes::Bytes::from_static(b"\x00\x00\x01\x01a\x00\x02\x01b\x01\x00\x01c\x02\x00");

        let raw = decode_ingest_body(&query, Some("application/octet-stream"), body, 1 << 20)
            .await
            .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();

        check!(
            (
                &raw.profile.sample_types()[0],
                values,
                ["a", "b", "c"]
                    .iter()
                    .all(|function| functions.contains(function)),
            ) == (
                &("samples".to_string(), "samples".to_string()),
                vec![1, 2],
                true,
            )
        );
    }

    #[tokio::test]
    async fn decode_plain_trie_format_payload_uses_serialized_folded_stack_trie() {
        let query = parse_ingest_query("name=myapp&format=trie&units=samples").unwrap();
        let body =
            bytes::Bytes::from_static(b"\x00\x00\x01\x02a;\x00\x02\x01b\x01\x00\x01c\x02\x00");

        let raw = decode_ingest_body(&query, Some("application/octet-stream"), body, 1 << 20)
            .await
            .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();

        check!(
            (
                &raw.profile.sample_types()[0],
                values,
                ["a", "b", "c"]
                    .iter()
                    .all(|function| functions.contains(function)),
            ) == (
                &("samples".to_string(), "samples".to_string()),
                vec![1, 2],
                true,
            )
        );
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_uses_until_as_profile_time() {
        let query =
            parse_ingest_query("name=myapp&from=1699999999000&until=1700000000000").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        assert!(raw.profile.inner().time_nanos == 1_700_000_000_000_000_000);
    }

    #[tokio::test]
    async fn decode_multipart_jfr_part_with_labels_as_folded_stacks() {
        let query = parse_ingest_query("name=myapp&format=jfr").unwrap();
        let boundary = "test-boundary";
        let folded =
            "java.lang.Thread.run;app.Worker.loop 11\njava.lang.Thread.run;app.Worker.idle 2\n";
        let labels = r#"{"service_name":"payments","region":"us-east"}"#;
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"labels\"\r\n");
        body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
        body.extend_from_slice(labels.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"jfr\"; filename=\"profile.jfr\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        check!(
            (
                raw.labels.get("__name__"),
                raw.labels.get("service_name"),
                raw.labels.get("region"),
                &raw.profile.sample_types()[0],
                raw.profile.samples().len(),
            ) == (
                Some("myapp"),
                Some("payments"),
                Some("us-east"),
                &("samples".to_string(), "count".to_string()),
                2,
            )
        );
    }

    #[tokio::test]
    async fn decode_multipart_jfr_binary_execution_samples() {
        let query = parse_ingest_query("name=myapp&format=jfr").unwrap();
        let boundary = "test-boundary";
        let jfr = include_bytes!("../../tests/fixtures/profiler-wall.jfr");
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"jfr\"; filename=\"profile.jfr\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(jfr);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            1 << 20,
        )
        .await
        .unwrap();

        assert_eq!(
            &raw.profile.sample_types()[0],
            &("wall".to_string(), "nanoseconds".to_string())
        );
        assert!(!raw.profile.samples().is_empty());
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();
        assert!(
            functions
                .iter()
                .any(|function| function.contains("CompileBroker::compiler_thread_loop"))
        );
    }

    /// LEB128 varint encoder mirroring [`read_tree_varint`], for crafting
    /// adversarial tree/trie payloads in the amplification tests below.
    fn put_tree_varint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = u8::try_from(value & 0x7f).unwrap();
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    #[test]
    fn tree_decoder_rejects_path_bytes_amplification() {
        // A single child node with a very long name that then declares a large
        // child count: the decoder would clone the long path once per declared
        // child (`repeat_n(path, children_len)`), amplifying memory far beyond
        // the ~17 KB input. The cumulative path-bytes budget must reject it as
        // a Decode error instead of OOMing.
        let long = 9_000_usize;
        let children = 8_000_u64;
        let mut body = Vec::new();
        // Root: empty name, no self value, exactly one child.
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, 1);
        // Child: long name, no self value, `children` declared children.
        put_tree_varint(&mut body, long as u64);
        body.extend(std::iter::repeat_n(b'a', long));
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, children);
        // Filler bytes so the per-node remaining-payload guard passes and the
        // path-bytes guard (not the cheap structural one) is what fires.
        body.extend(std::iter::repeat_n(
            0_u8,
            usize::try_from(children).unwrap(),
        ));

        let err = tree_to_pprof("app", "samples", &body).unwrap_err();
        assert!(matches!(err, ProfilesError::Decode(_)));

        // Sanity: a normal small tree still decodes successfully.
        let ok = b"\x00\x00\x01\x01a\x00\x02\x01b\x01\x00\x01c\x02\x00";
        assert!(tree_to_pprof("app", "samples", ok).is_ok());
    }

    #[test]
    fn trie_decoder_rejects_deep_payload_past_depth_cap() {
        // A linear chain of single-child nodes deeper than MAX_TRIE_DEPTH. The
        // old recursive `parse_trie_node` would recurse once per level and blow
        // the native stack; the explicit work-stack must reject past the cap.
        let depth = MAX_TRIE_DEPTH + 16;
        let mut body = Vec::new();
        for _ in 0..depth - 1 {
            // suffix "a", value 0, one child.
            put_tree_varint(&mut body, 1);
            body.push(b'a');
            put_tree_varint(&mut body, 0);
            put_tree_varint(&mut body, 1);
        }
        // Leaf: suffix "a", value 1, no children.
        put_tree_varint(&mut body, 1);
        body.push(b'a');
        put_tree_varint(&mut body, 1);
        put_tree_varint(&mut body, 0);

        let err = trie_to_pprof("app", "samples", &body).unwrap_err();
        assert!(matches!(err, ProfilesError::Decode(_)));

        // Sanity: a chain comfortably under the cap still decodes.
        let shallow_depth = 64_usize;
        let mut shallow = Vec::new();
        for _ in 0..shallow_depth - 1 {
            put_tree_varint(&mut shallow, 1);
            shallow.push(b'a');
            put_tree_varint(&mut shallow, 0);
            put_tree_varint(&mut shallow, 1);
        }
        put_tree_varint(&mut shallow, 1);
        shallow.push(b'a');
        put_tree_varint(&mut shallow, 1);
        put_tree_varint(&mut shallow, 0);
        assert!(trie_to_pprof("app", "samples", &shallow).is_ok());

        // Sanity: the canonical small trie payload still decodes.
        let ok = b"\x00\x00\x01\x02a;\x00\x02\x01b\x01\x00\x01c\x02\x00";
        assert!(trie_to_pprof("app", "samples", ok).is_ok());
    }
}

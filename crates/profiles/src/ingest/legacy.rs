//! Legacy `POST /ingest` door.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::Cursor;

use crate::error::ProfilesError;
use crate::ingest::RawProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFormat {
    Pprof,
    Jfr,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQuery {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub format: IngestFormat,
    pub sample_rate: u32,
    pub units: String,
}

pub fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError> {
    let mut name = String::new();
    let mut labels = Vec::new();
    let mut format = IngestFormat::Groups;
    let mut sample_rate = 100;
    let mut units = "count".to_string();

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
            "profile" | "groups" | "folded" if query.format == IngestFormat::Groups => {
                folded_bytes = Some(data.to_vec());
            }
            "jfr" if query.format == IngestFormat::Jfr => jfr_bytes = Some(data.to_vec()),
            "labels" if query.format == IngestFormat::Jfr => {
                multipart_labels = parse_labels_part(&data)?;
            }
            _ => {}
        }
    }

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
        IngestFormat::Jfr => {
            let raw = jfr_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart `jfr` part".to_string())
            })?;
            jfr_to_pprof(&query.name, &raw)?
        }
    };
    let mut labels = Labels::new();
    labels.insert("__name__", query.name.clone());
    for (name, value) in &query.labels {
        labels.insert(name.clone(), value.clone());
    }
    for (name, value) in multipart_labels {
        labels.insert(name, value);
    }

    Ok(RawProfile { labels, profile })
}

#[derive(Debug, Deserialize)]
struct SampleTypeConfig {
    units: Option<String>,
    #[serde(rename = "display-name")]
    display_name: Option<String>,
    #[allow(dead_code)]
    aggregation: Option<String>,
    #[allow(dead_code)]
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
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_query_extracts_name_labels_format() {
        let q =
            parse_ingest_query("name=myapp{env=\"prod\",team=\"core\"}&format=pprof&sampleRate=97")
                .unwrap();

        assert!(q.name == "myapp");
        assert!(q.labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(matches!(q.format, IngestFormat::Pprof));
        assert!(q.sample_rate == 97);
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

        assert!(raw.labels.get("__name__") == Some("myapp"));
        assert!(raw.labels.get("env") == Some("prod"));
        assert!(raw.profile.sample_types()[0].0 == "cpu");
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

        assert!(raw.profile.sample_types()[0] == ("wall".to_string(), "nanoseconds".to_string()));
        assert!(
            raw.profile.period_type_strings() == ("wall".to_string(), "nanoseconds".to_string())
        );
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

        assert!(raw.labels.get("__name__") == Some("myapp"));
        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "count".to_string()));
        assert!(raw.profile.samples().len() == 2);
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

        assert!(raw.labels.get("__name__") == Some("myapp"));
        assert!(raw.labels.get("service_name") == Some("payments"));
        assert!(raw.labels.get("region") == Some("us-east"));
        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "count".to_string()));
        assert!(raw.profile.samples().len() == 2);
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

        assert!(raw.profile.sample_types()[0] == ("wall".to_string(), "nanoseconds".to_string()));
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
}

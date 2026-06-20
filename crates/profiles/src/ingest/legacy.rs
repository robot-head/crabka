//! Legacy `POST /ingest` door.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

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
}

pub fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError> {
    let mut name = String::new();
    let mut labels = Vec::new();
    let mut format = IngestFormat::Groups;
    let mut sample_rate = 100;

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
    })
}

pub async fn decode_ingest_multipart(
    query: &IngestQuery,
    content_type: &str,
    body: bytes::Bytes,
    max: usize,
) -> Result<RawProfile, ProfilesError> {
    if query.format != IngestFormat::Pprof {
        // TODO(slice4-ingest-jfr): add JFR chunk decoding.
        // TODO(slice4-ingest-folded): add folded/group/tree format decoding.
        return Err(ProfilesError::UnsupportedFormat(format!(
            "{:?}",
            query.format
        )));
    }

    let boundary =
        multer::parse_boundary(content_type).map_err(|e| ProfilesError::Invalid(e.to_string()))?;
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut pprof_bytes = None;

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
        if name == "profile" {
            pprof_bytes = Some(data.to_vec());
        }
    }

    let raw = pprof_bytes
        .ok_or_else(|| ProfilesError::Invalid("missing multipart `profile` part".to_string()))?;
    let profile = PprofProfile::decode(&raw)?;
    let mut labels = Labels::new();
    labels.insert("__name__", query.name.clone());
    for (name, value) in &query.labels {
        labels.insert(name.clone(), value.clone());
    }

    Ok(RawProfile { labels, profile })
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
}

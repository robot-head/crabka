//! Generate the operator CRD reference pages from `K::crd()`.

use crabka_operator::crd::{
    Kafka, KafkaNodePool, KafkaRebalance, KafkaTopic, KafkaUser, SchemaRegistry,
};
use kube::CustomResourceExt;
use serde_json::Value;

use crate::schema_md::render_field_table;

/// One generated CRD page: front-matter title/weight metadata and body markdown.
pub struct CrdPage {
    pub slug: String,
    pub title: String,
    pub body: String,
}

/// Build a reference page for every CRD the operator owns.
#[must_use]
pub fn crd_pages() -> Vec<CrdPage> {
    vec![
        page::<Kafka>(),
        page::<KafkaNodePool>(),
        page::<KafkaTopic>(),
        page::<KafkaUser>(),
        page::<KafkaRebalance>(),
        page::<SchemaRegistry>(),
    ]
}

fn page<K: CustomResourceExt>() -> CrdPage {
    let crd = K::crd();
    let kind = crd.spec.names.kind.clone();
    let group = crd.spec.group.clone();
    let version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.storage)
        .or_else(|| crd.spec.versions.first())
        .expect("CRD has >=1 version");
    let schema_json: Value = version
        .schema
        .as_ref()
        .and_then(|s| s.open_api_v3_schema.as_ref())
        .map_or(Value::Null, |s| {
            serde_json::to_value(s).expect("schema to json")
        });
    let spec = schema_json
        .pointer("/properties/spec")
        .cloned()
        .unwrap_or(Value::Null);
    let status = schema_json.pointer("/properties/status").cloned();
    let mut body = format!(
        "**API group/version:** `{group}/{}`\n\n## Spec\n\n",
        version.name
    );
    body.push_str(&render_field_table(&spec));
    if let Some(status) = status
        && status.get("properties").is_some()
    {
        body.push_str("\n## Status\n\n");
        body.push_str(&render_field_table(&status));
    }
    CrdPage {
        slug: kind.to_lowercase(),
        title: kind,
        body,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    #[test]
    fn kafka_page_has_spec_fields() {
        let pages = crd_pages();
        let kafka = pages
            .iter()
            .find(|p| p.slug == "kafka")
            .expect("kafka page");
        assert_eq!(kafka.title, "Kafka");
        check!(
            kafka
                .body
                .contains("| Field | Type | Required | Default | Description |")
        );
        // Guard against render_field_table silently producing an empty table:
        // a concrete, stable field from the Kafka CRD spec must be present.
        check!(
            kafka.body.contains("| `kafkaVersion` |"),
            "expected kafkaVersion field in kafka spec table:\n{}",
            kafka.body
        );
        assert_eq!(pages.len(), 6);
        let slugs: Vec<&str> = pages.iter().map(|p| p.slug.as_str()).collect();
        for e in [
            "kafka",
            "kafkanodepool",
            "kafkatopic",
            "kafkauser",
            "kafkarebalance",
            "schemaregistry",
        ] {
            assert!(slugs.contains(&e), "missing {e}");
        }
    }
}

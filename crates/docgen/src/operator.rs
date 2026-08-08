//! Generate the operator CRD reference pages from `K::crd()`.

use crabka_operator::crd::{
    Gres, GresTenant, Kafka, KafkaGrpcGateway, KafkaNodePool, KafkaRebalance, KafkaTopic,
    KafkaUser, SchemaRegistry,
};
use kube::CustomResourceExt;
use serde_json::Value;

use crate::schema_md::render_field_table;

/// One generated CRD page: the front-matter title and weight metadata, plus the
/// body markdown.
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
        page::<KafkaGrpcGateway>(),
        page::<SchemaRegistry>(),
        // Order is the only thing setting each page's weight (`emit` assigns
        // `(index + 1) * 10`), so this list is grouped the way the nav should
        // read: the Kafka family, then the standalone services, then Gres.
        // Reordering only moves pages in the nav — a page's URL comes from its
        // slug, so no links break.
        page::<Gres>(),
        page::<GresTenant>(),
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
    use assert2::check;

    use super::*;
    #[test]
    fn kafka_page_has_spec_fields() {
        let pages = crd_pages();
        let kafka = pages
            .iter()
            .find(|p| p.slug == "kafka")
            .expect("kafka page");
        assert2::assert!(kafka.title == "Kafka");
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
        assert2::assert!(pages.len() == 9);
        let slugs: Vec<&str> = pages.iter().map(|p| p.slug.as_str()).collect();
        for e in [
            "kafka",
            "kafkanodepool",
            "kafkatopic",
            "kafkauser",
            "kafkarebalance",
            "kafkagrpcgateway",
            "schemaregistry",
            "gres",
            "grestenant",
        ] {
            assert2::assert!(slugs.contains(&e));
        }
    }

    /// The pages added after the original six. Each one has the same guard as
    /// the Kafka page: a concrete field must appear. If `render_field_table`
    /// silently emits an empty table, this test fails and no blank page ships.
    #[test]
    fn the_later_pages_have_spec_fields() {
        let pages = crd_pages();
        for (slug, title, field) in [
            // `tracing` is the field the Gres page was added to surface;
            // `ranges` and `replicas` are long-standing, so together they catch
            // both an empty table and a page built from a stale schema.
            ("gres", "Gres", "| `tracing` |"),
            ("grestenant", "GresTenant", "| `ranges` |"),
            ("kafkagrpcgateway", "KafkaGrpcGateway", "| `replicas` |"),
        ] {
            let page = pages
                .iter()
                .find(|p| p.slug == slug)
                .unwrap_or_else(|| panic!("{slug} page"));
            check!(page.title == title);
            check!(
                page.body
                    .contains("| Field | Type | Required | Default | Description |"),
                "expected a field table on {slug}:\n{}",
                page.body
            );
            check!(
                page.body.contains(field),
                "expected {field} in {slug} spec table:\n{}",
                page.body
            );
        }
    }

    /// `emit` derives each page's weight from its index. A reordering that
    /// looks harmless thus renumbers every page after it and moves the site
    /// nav. This test pins the order, not only the membership.
    #[test]
    fn page_order_fixes_the_generated_weights() {
        let slugs: Vec<String> = crd_pages().into_iter().map(|p| p.slug).collect();
        check!(
            slugs
                == [
                    "kafka",
                    "kafkanodepool",
                    "kafkatopic",
                    "kafkauser",
                    "kafkarebalance",
                    "kafkagrpcgateway",
                    "schemaregistry",
                    "gres",
                    "grestenant",
                ]
        );
    }
}

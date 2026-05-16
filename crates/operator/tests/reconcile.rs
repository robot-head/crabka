//! Mocked-client reconcile tests. We bypass the real API server and assert
//! that `reconcile` issues a PATCH against the Kafka status subresource
//! with the expected `Ready=True` condition.

use std::sync::Arc;

use crabka_operator::config::OperatorConfig;
use crabka_operator::context::Context;
use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec};
use crabka_operator::telemetry::new_registry;
use http::{Request, Response};
use http_body_util::BodyExt as _;
use hyper::body::Bytes;
use kube::Client;
use tokio::sync::{Mutex, mpsc};
use tower::ServiceBuilder;
use tower::service_fn;

#[tokio::test]
async fn reconcile_patches_status_ready_true() {
    // Channel the mock service uses to surface the captured request to
    // the test body. Bytes so the test can decode the request body.
    let (tx, mut rx) = mpsc::unbounded_channel::<Request<Bytes>>();

    let service = ServiceBuilder::new().service(service_fn(
        move |req: Request<kube::client::Body>| {
            let tx = tx.clone();
            async move {
                let (parts, body) = req.into_parts();
                let bytes = body.collect().await.unwrap().to_bytes();
                tx.send(Request::from_parts(parts, bytes)).unwrap();
                // Return the patched object so kube can deserialize it.
                Ok::<_, kube::Error>(
                    Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            br#"{"apiVersion":"crabka.io/v1alpha1","kind":"Kafka","metadata":{"name":"x","namespace":"y","uid":"u"},"spec":{"kafkaVersion":"0.1.1"},"status":{"conditions":[{"type":"Ready","status":"True","reason":"Stub","message":"","lastTransitionTime":"2026-05-16T00:00:00Z"}]}}"#.to_vec(),
                        ))
                        .unwrap(),
                )
            }
        },
    ));
    let client = Client::new(service, "y");
    let ctx = Context::new(
        client,
        OperatorConfig {
            watch_namespaces: vec![],
            operator_namespace: "y".into(),
            lease_name: "l".into(),
            pod_name: "p".into(),
            health_addr: "0.0.0.0:0".parse().unwrap(),
            log_filter: "info".into(),
        },
        Arc::new(Mutex::new(new_registry())),
    );

    let mut kafka = Kafka::new(
        "x",
        KafkaSpec {
            kafka_version: "0.1.1".into(),
        },
    );
    kafka.metadata.namespace = Some("y".into());

    reconcile(Arc::new(kafka), Arc::new(ctx)).await.unwrap();

    // Find the PATCH request and verify it targets the status subresource
    // and includes a Ready=True condition.
    let req = rx.recv().await.expect("expected a request");
    assert_eq!(req.method(), http::Method::PATCH);
    let uri = req.uri().to_string();
    assert!(
        uri.contains("/apis/crabka.io/v1alpha1/namespaces/y/kafkas/x/status"),
        "uri = {uri}"
    );
    let body = std::str::from_utf8(req.body()).unwrap();
    assert!(body.contains("\"type\":\"Ready\""), "body = {body}");
    assert!(body.contains("\"status\":\"True\""), "body = {body}");
}

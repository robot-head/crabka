use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to Crabka broker at 127.0.0.1:9092...");
    let mut admin = AdminClient::connect(&["127.0.0.1:9092".to_string()]).await?;
    println!("Connected successfully! Creating topic 'test-topic'...");

    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "test-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            30_000,
        )
        .await?;
    println!("Topic 'test-topic' created successfully! Fetching metadata...");

    let metadata = admin.metadata(&["test-topic"]).await?;
    println!("Fetched metadata for 'test-topic': {:#?}", metadata.topics);

    Ok(())
}

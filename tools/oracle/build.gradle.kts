plugins {
    java
    application
}

repositories { mavenCentral() }

dependencies {
    // Match the Kafka version pinned in crates/protocol/schemas/VERSION.
    implementation("org.apache.kafka:kafka-clients:4.2.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

application { mainClass.set("com.crabka.oracle.Oracle") }

plugins {
    java
    application
}

repositories { mavenCentral() }

dependencies {
    // Match the Kafka version pinned in crates/protocol/schemas/VERSION.
    implementation("org.apache.kafka:kafka-clients:4.2.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
    // Transitive compression codec deps — needed at compile time for the
    // compress/decompress oracle ops added in Task 9.
    implementation("org.xerial.snappy:snappy-java:1.1.10.8")
    implementation("com.github.luben:zstd-jni:1.5.7-8")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

application { mainClass.set("com.crabka.oracle.Oracle") }

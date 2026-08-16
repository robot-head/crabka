plugins {
    `java-library`
    kotlin("jvm") version "2.4.10"
    application
}

repositories { mavenCentral() }

dependencies {
    implementation("com.connectrpc:connect-kotlin:0.9.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
    implementation("com.google.protobuf:protobuf-java:4.35.1")
    implementation("com.squareup.okhttp3:okhttp:5.4.0")

    testImplementation("com.squareup.okhttp3:mockwebserver3:5.5.0")
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

sourceSets {
    main {
        java.srcDir("gen")
        kotlin.srcDir("gen")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
}

application { mainClass.set("dev.crabka.sdk.AdapterMain") }

tasks.test { useJUnitPlatform() }

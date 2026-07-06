# The Java SDK — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Language cycle under the [application-SDK umbrella](2026-07-06-crabka-app-sdk-umbrella-design.md). Implements contract v1 on the JVM; definition of done = the conformance suite green through the JVM adapter.

## Context

The JVM is the one non-Rust ecosystem the repo already builds in CI (the Gradle-driven JVM differential oracle under `tools/oracle`), which makes Java the cheapest breadth win after TS. The Connect protocol's only JVM implementation is **connect-kotlin** (Buf's), which generates Kotlin APIs — so the load-bearing decision here is how a *Java-first* SDK sits on a Kotlin transport.

## The Kotlin-core / Java-facade decision

The SDK is implemented as a **Kotlin core** (using connect-kotlin's generated clients and okhttp transport) wrapped in a **deliberately Java-idiomatic facade**: builder-pattern construction, `CompletableFuture` for async, checked-exception-free error hierarchy, `Iterator`/`Stream` subscribe surface, `@JvmStatic`/`@JvmOverloads` throughout, and the public API defined as Java-visible types only (no suspend functions, no Kotlin-only types across the boundary). Java consumers never see Kotlin idioms; Kotlin consumers get the core for free. *Alternative rejected — pure-Java hand-rolled Connect client:* re-implements framing/streaming okhttp+connect-kotlin already solve, for no consumer-visible gain.

## Design Goals

- **connect-kotlin + okhttp** with `Protocol.H2_PRIOR_KNOWLEDGE` for cleartext h2 (the bidi `Subscribe` requirement against the gateway's h2c listener).
- **The umbrella contract verbatim:** six modules; `CrabkaException` hierarchy with `UnimplementedException { module, gatedOn }` carrying the pinned slugs; CE mapping identical (vector-pinned).
- **Gradle-native:** `sdks/java/` is a standalone Gradle project (Kotlin JVM plugin + `buf` codegen via the gradle plugin or a generate task invoking the repo buf config); the adapter is a `application`-plugin CLI (`installDist` — the same shape CI already runs for the oracle).

## Non-goals

Android (okhttp works there but the h2c/serverless posture doesn't — out of scope); Maven Central publishing (deferred with SDK release engineering); reactive-streams bindings (a `Flow`/Reactor adapter is a later ergonomic add); manual per-offset ack beyond the experimental flag.

## Architecture

```
sdks/java/                          (Gradle; group dev.crabka, artifact crabka-sdk)
├── build.gradle.kts / settings.gradle.kts
├── gen/                            connect-kotlin + protobuf-kotlin output (drift-checked)
├── src/main/kotlin/dev/crabka/sdk/
│   ├── internal/…                  the Kotlin core: transport, streams, CE mapping
│   ├── CrabkaClient.java-facing    builder → modules; CompletableFuture async surface
│   ├── errors: CrabkaException ⊃ TransportException | UnauthenticatedException | …
│   │           | UnimplementedException(module, gatedOn)
│   ├── Messaging: publish / publishEvent / subscribe → MessageStream (Iterator<Inbound>, AutoCloseable)
│   └── stubs: Queues / Database / Blob (pinned slugs); Auth = credential config only
├── src/main/kotlin/dev/crabka/sdk/conformance/AdapterMain.kt   (JSON-lines stdio CLI)
└── src/test/kotlin | src/test/java  (JUnit5 — Java-facade tests written IN JAVA to prove the facade)
```

**Facade proof discipline:** the unit tests for the public surface are written **in Java**, not Kotlin — if the API isn't pleasant from Java, its own tests hurt first.

## Integration

- **buf codegen:** add the `buf.build/connectrpc/kotlin` + `buf.build/protocolbuffers/kotlin` (or java) plugin blocks, `out: sdks/java/gen`; the drift check extends.
- **CI:** reuses the existing Gradle/JVM toolchain (the oracle's `setup-java` pattern); an `sdk-java` job: `gradle build test installDist` → run the conformance suite with `--adapter sdks/java/build/install/adapter/bin/adapter`.
- **The harness/vectors unchanged.**

## Testing

Java-written facade units (builder, taxonomy mapping, CE mapping via the core); Kotlin core units where internal; **the conformance suite is the gate** — all v1 vectors green through the JVM adapter.

## Risks

- **connect-kotlin h2c:** okhttp's `H2_PRIOR_KNOWLEDGE` against the gateway's `auto::Builder` listener is the cycle's smoke-first verification (same discipline as TS).
- **Kotlin-in-the-repo precedent:** the repo's JVM code so far is the oracle tooling; the SDK adds Kotlin as a shipped-artifact language — contained to `sdks/java`, outside the Rust workspace, mirrored on the sdks/ rule.
- **connect-kotlin maturity vs Connect-ES** — streaming-API ergonomics are rougher; wrapped entirely by the facade, so churn stays internal.

## Resolved decisions

Kotlin core + Java-first facade (Java-written facade tests as the enforcement); okhttp h2-prior-knowledge; Gradle standalone project reusing CI's JVM toolchain; the umbrella taxonomy/slugs verbatim; Android + publishing deferred.

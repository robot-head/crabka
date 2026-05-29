+++
title = "Crabka"
sort_by = "weight"

[extra]
lead = "A Rust reimplementation of Apache Kafka — wire-protocol exact, KRaft metadata, Kubernetes operator."
url = "/guide/introduction/"
url_button = "Get started"
repo_url = "https://github.com/robot-head/crabka"
repo_license = "Apache 2.0"
repo_version = "0.1"

[[extra.menu.main]]
name = "Guide"
url = "/guide/"
section = "guide"
weight = 10

[[extra.menu.main]]
name = "Benchmarks"
url = "/benchmarks/"
section = "benchmarks"
weight = 15

[[extra.menu.main]]
name = "Reference"
url = "/reference/"
section = "reference"
weight = 20

# Homepage feature cards (rendered under the hero). Benchmark headlines
# first — see /benchmarks/ for the full methodology and tables.
[[extra.list]]
title = "1.3–1.5× the throughput"
content = 'Higher producer throughput than <strong>Apache Kafka 4.3</strong> across every scenario on identical hardware, at lower p99 ack latency. <a href="/benchmarks/crabka-vs-kafka/">See the benchmarks →</a>'

[[extra.list]]
title = "~40× less memory"
content = "Broker resident in <strong>19–26 MiB</strong> versus Kafka's ~1 GiB JVM footprint — no heap to tune, no GC pauses."

[[extra.list]]
title = "Ready in 1–2 seconds"
content = "Cold start to first ack in <strong>1–2 s</strong>, versus 8–9 s for a Kafka broker on the same box."

[[extra.list]]
title = "Wire-protocol exact"
content = "Speaks the Kafka protocol byte-for-byte and works with the JVM admin tools — the benchmarks above use the stock <code>kafka-topics.sh</code> against both stacks."
+++

A Rust reimplementation of Apache Kafka. Speaks the Kafka wire protocol
byte-for-byte, runs its metadata quorum on KRaft, and ships a Kubernetes
operator and a Cruise-Control-equivalent rebalancer.

# Crabka vs Strimzi benchmark — results

Each scenario was run once per stack. The `ratio` column is `crabka / kafka` for throughput / efficiency (higher is better for Crabka) and `kafka / crabka` for latency / resource (lower-is-better Crabka still > 1).

## `failover`

Topology: partitions=12, RF=3, broker_count=3 (per stack). Duration=180s, warmup=30s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 0.000 | 6101.122 | 0.00× |
| consumer msgs/s (higher better) | 0.000 | 1049.489 | 0.00× |
| producer MB/s (higher better) | 0.000 | 5.958 | 0.00× |
| p99 producer ack ms (lower better) | 0.000 | 274.431 | — |
| p99 consumer e2e ms (lower better) | 0.000 | 610.815 | — |
| msgs/s per CPU-core (higher better) | 0.000 | 2996.047 | 0.00× |
| cgroup working-set MB (lower better) | 0.000 | 8401.324 | — |
| startup ms (CR-apply → Ready) (lower better) | 0.000 | 66936.000 | — |
| first-ack ms (Ready → first ack) (lower better) | 0.000 | 227.000 | — |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 0.000 | 160.767 |
| p95 | 0.000 | 212.095 |
| p99 | 0.000 | 274.431 |
| p99.9 | 0.000 | 658.431 |
| max | 0.000 | 719.871 |
| count | 0.000 | 1098202.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 0.000 | 294.655 |
| p95 | 0.000 | 400.639 |
| p99 | 0.000 | 610.815 |
| p99.9 | 0.000 | 777.215 |
| max | 0.000 | 811.519 |
| count | 0.000 | 188908.000 |

**Kafka memory split (MiB):**

- JVM heap used: 4204.3
- JVM non-heap used: 261.9
- Page-cache (approx, working-set − heap − non-heap): 3935.1
- cgroup working-set (limit-relevant): 8401.3

**Failover (kafka):** recovery in 0 ms, 0 drops, max latency spike 0.0 ms.

_kafka errors:_ consumer-0-poll: client: connection closed


## `fan-out`

Topology: partitions=24, RF=1, broker_count=3 (per stack). Duration=120s, warmup=15s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 71141.642 | 42667.708 | 1.67× |
| consumer msgs/s (higher better) | 8949.850 | 8137.650 | 1.10× |
| producer MB/s (higher better) | 69.474 | 41.668 | 1.67× |
| p99 producer ack ms (lower better) | 70.143 | 196.351 | 2.80× |
| p99 consumer e2e ms (lower better) | 18792.447 | 60030.975 | 3.19× |
| msgs/s per CPU-core (higher better) | 44672.223 | 22472.558 | 1.99× |
| cgroup working-set MB (lower better) | 266.242 | 5027.383 | 18.88× |
| startup ms (CR-apply → Ready) (lower better) | 29370.000 | 61359.000 | 2.09× |
| first-ack ms (Ready → first ack) (lower better) | 31.000 | 110.000 | 3.55× |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 26.335 | 41.823 |
| p95 | 43.935 | 71.935 |
| p99 | 70.143 | 196.351 |
| p99.9 | 122.623 | 333.311 |
| max | 154.623 | 463.103 |
| count | 8536997.000 | 5120125.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 6541.311 | 58884.095 |
| p95 | 14417.919 | 60030.975 |
| p99 | 18792.447 | 60030.975 |
| p99.9 | 19726.335 | 60030.975 |
| max | 19824.639 | 60030.975 |
| count | 1073982.000 | 976518.000 |

**Kafka memory split (MiB):**

- JVM heap used: 2266.9
- JVM non-heap used: 262.6
- Page-cache (approx, working-set − heap − non-heap): 2497.9
- cgroup working-set (limit-relevant): 5027.4


## `fixed-rate-latency`

Topology: partitions=6, RF=1, broker_count=3 (per stack). Duration=120s, warmup=15s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 10165.600 | 6384.067 | 1.59× |
| consumer msgs/s (higher better) | 8992.517 | 6389.842 | 1.41× |
| producer MB/s (higher better) | 9.927 | 6.234 | 1.59× |
| p99 producer ack ms (lower better) | 97.023 | 110.207 | 1.14× |
| p99 consumer e2e ms (lower better) | 14344.191 | 301.311 | 0.02× |
| msgs/s per CPU-core (higher better) | 21112.437 | 7389.697 | 2.86× |
| cgroup working-set MB (lower better) | 112.078 | 2550.477 | 22.76× |
| startup ms (CR-apply → Ready) (lower better) | 29212.000 | 66739.000 | 2.28× |
| first-ack ms (Ready → first ack) (lower better) | 34.000 | 216.000 | 6.35× |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 48.671 | 79.551 |
| p95 | 62.559 | 95.743 |
| p99 | 97.023 | 110.207 |
| p99.9 | 109.119 | 184.831 |
| max | 199.167 | 235.007 |
| count | 1219872.000 | 766088.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 9175.039 | 205.439 |
| p95 | 13156.351 | 283.647 |
| p99 | 14344.191 | 301.311 |
| p99.9 | 14589.951 | 361.727 |
| max | 14639.103 | 397.311 |
| count | 1079102.000 | 766781.000 |

**Kafka memory split (MiB):**

- JVM heap used: 1053.2
- JVM non-heap used: 260.2
- Page-cache (approx, working-set − heap − non-heap): 1237.1
- cgroup working-set (limit-relevant): 2550.5


## `large-msg`

Topology: partitions=6, RF=1, broker_count=3 (per stack). Duration=60s, warmup=10s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 3035.200 | 4363.200 | 0.70× |
| consumer msgs/s (higher better) | 3035.267 | 4650.033 | 0.65× |
| producer MB/s (higher better) | 296.406 | 426.094 | 0.70× |
| p99 producer ack ms (lower better) | 234.367 | 289.791 | 1.24× |
| p99 consumer e2e ms (lower better) | 483.839 | 4624.383 | 9.56× |
| msgs/s per CPU-core (higher better) | 7025.980 | 6018.170 | 1.17× |
| cgroup working-set MB (lower better) | 113.645 | 5151.398 | 45.33× |
| startup ms (CR-apply → Ready) (lower better) | 29280.000 | 66767.000 | 2.28× |
| first-ack ms (Ready → first ack) (lower better) | 51.000 | 122.000 | 2.39× |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 171.391 | 94.143 |
| p95 | 211.711 | 203.007 |
| p99 | 234.367 | 289.791 |
| p99.9 | 319.999 | 357.631 |
| max | 330.495 | 360.191 |
| count | 182112.000 | 261792.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 333.823 | 454.911 |
| p95 | 448.255 | 3917.823 |
| p99 | 483.839 | 4624.383 |
| p99.9 | 534.015 | 4780.031 |
| max | 609.279 | 4808.703 |
| count | 182116.000 | 279002.000 |

**Kafka memory split (MiB):**

- JVM heap used: 1006.1
- JVM non-heap used: 253.4
- Page-cache (approx, working-set − heap − non-heap): 3891.9
- cgroup working-set (limit-relevant): 5151.4


## `mixed-acks-all`

Topology: partitions=6, RF=1, broker_count=3 (per stack). Duration=120s, warmup=15s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 19961.492 | 19844.017 | 1.01× |
| consumer msgs/s (higher better) | 9002.042 | 8780.925 | 1.03× |
| producer MB/s (higher better) | 19.494 | 19.379 | 1.01× |
| p99 producer ack ms (lower better) | 9.783 | 31.263 | 3.20× |
| p99 consumer e2e ms (lower better) | 29245.439 | 60030.975 | 2.05× |
| msgs/s per CPU-core (higher better) | 34042.355 | 16781.650 | 2.03× |
| cgroup working-set MB (lower better) | 114.395 | 4249.152 | 37.14× |
| startup ms (CR-apply → Ready) (lower better) | 29323.000 | 72290.000 | 2.47× |
| first-ack ms (Ready → first ack) (lower better) | 40.000 | 218.000 | 5.45× |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 1.670 | 1.814 |
| p95 | 2.885 | 6.943 |
| p99 | 9.783 | 31.263 |
| p99.9 | 82.111 | 141.311 |
| max | 160.383 | 192.255 |
| count | 2395379.000 | 2381282.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 14303.231 | 41877.503 |
| p95 | 27607.039 | 60030.975 |
| p99 | 29245.439 | 60030.975 |
| p99.9 | 29835.263 | 60030.975 |
| max | 29900.799 | 60030.975 |
| count | 1080245.000 | 1053711.000 |

**Kafka memory split (MiB):**

- JVM heap used: 2272.7
- JVM non-heap used: 260.8
- Page-cache (approx, working-set − heap − non-heap): 1715.6
- cgroup working-set (limit-relevant): 4249.2


## `small-msg-saturate`

Topology: partitions=6, RF=1, broker_count=3 (per stack). Duration=60s, warmup=10s.

| metric | crabka | kafka | ratio |
|---|---|---|---|
| producer msgs/s (higher better) | 118364.900 | 117389.883 | 1.01× |
| consumer msgs/s (higher better) | 81456.850 | 77993.950 | 1.04× |
| producer MB/s (higher better) | 11.288 | 11.195 | 1.01× |
| p99 producer ack ms (lower better) | 7.923 | 9.615 | 1.21× |
| p99 consumer e2e ms (lower better) | 25509.887 | 22429.695 | 0.88× |
| msgs/s per CPU-core (higher better) | 261361.433 | 105760.945 | 2.47× |
| cgroup working-set MB (lower better) | 53.449 | 2520.699 | 47.16× |
| startup ms (CR-apply → Ready) (lower better) | 34822.000 | 66868.000 | 1.92× |
| first-ack ms (Ready → first ack) (lower better) | 56.000 | 90.000 | 1.61× |

**Producer ack latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 4.127 | 4.067 |
| p95 | 5.103 | 6.011 |
| p99 | 7.923 | 9.615 |
| p99.9 | 11.359 | 27.967 |
| max | 140.927 | 74.367 |
| count | 7101894.000 | 7043393.000 |

**Consumer end-to-end latency (ms):**

| percentile | crabka | kafka |
|---|---|---|
| p50 | 16416.767 | 11493.375 |
| p95 | 24723.455 | 21676.031 |
| p99 | 25509.887 | 22429.695 |
| p99.9 | 25690.111 | 22593.535 |
| max | 25722.879 | 22642.687 |
| count | 4887411.000 | 4679637.000 |

**Kafka memory split (MiB):**

- JVM heap used: 1008.8
- JVM non-heap used: 252.1
- Page-cache (approx, working-set − heap − non-heap): 1259.8
- cgroup working-set (limit-relevant): 2520.7



#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kafka_ref="${KAFKA_REF:-b475ce7632bc661a79b61ee8b78124bc53170bca}"
kafka_dir="${KAFKA_DIR:-${repo_root}/target/kafka-conformance/kafka-${kafka_ref:0:12}}"
report_dir="${repo_root}/target/kafka-conformance"
report="${report_dir}/report.md"

mkdir -p "$report_dir"
if [[ ! -d "$kafka_dir/.git" ]]; then
    mkdir -p "$kafka_dir"
    git -C "$kafka_dir" init
    git -C "$kafka_dir" remote add origin https://github.com/apache/kafka.git
    git -C "$kafka_dir" fetch --depth 1 origin "$kafka_ref"
    git -C "$kafka_dir" checkout --detach FETCH_HEAD
fi
if [[ "$(git -C "$kafka_dir" rev-parse HEAD)" != "$kafka_ref" ]]; then
    echo "Kafka checkout $kafka_dir is not at $kafka_ref" >&2
    exit 2
fi

cargo build --release --locked -p crabka-broker -p crabka-cli
cp "$repo_root/target/release/crabka-broker" "$kafka_dir/crabka-broker"
cp "$repo_root/target/release/crabka" "$kafka_dir/crabka"

tests=(
    "console-consumer|tests/kafkatest/sanity_checks/test_console_consumer.py::ConsoleConsumerTest.test_lifecycle|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "verifiable-producer|tests/kafkatest/sanity_checks/test_verifiable_producer.py::TestVerifiableProducer.test_simple_run|{\"producer_version\":\"dev\",\"acks\":\"-1\",\"enable_idempotence\":true,\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "compression|tests/kafkatest/tests/client/compression_test.py::CompressionTest.test_compressed_topic|{\"compression_types\":[\"snappy\",\"gzip\",\"lz4\",\"zstd\",\"none\"],\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "get-offset-shell|tests/kafkatest/tests/core/get_offset_shell_test.py::GetOffsetShellTest.test_get_offset_shell_topic_name|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "client-compatibility|tests/kafkatest/tests/client/client_compatibility_features_test.py::ClientCompatibilityFeaturesTest.run_compatibility_test|{\"broker_version\":\"dev\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "get-offset-pattern|tests/kafkatest/tests/core/get_offset_shell_test.py::GetOffsetShellTest.test_get_offset_shell_topic_pattern|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "get-offset-partitions|tests/kafkatest/tests/core/get_offset_shell_test.py::GetOffsetShellTest.test_get_offset_shell_partitions|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "get-offset-topic-partitions|tests/kafkatest/tests/core/get_offset_shell_test.py::GetOffsetShellTest.test_get_offset_shell_topic_partitions|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "get-offset-internal-filter|tests/kafkatest/tests/core/get_offset_shell_test.py::GetOffsetShellTest.test_get_offset_shell_internal_filter|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "console-share-consumer|tests/kafkatest/sanity_checks/test_console_share_consumer.py::ConsoleShareConsumerTest.test_lifecycle|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "pluggable-consumer|tests/kafkatest/tests/client/pluggable_test.py::PluggableConsumerTest.test_start_stop|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "connect-standalone|tests/kafkatest/tests/connect/connect_test.py::ConnectStandaloneFileTest.test_file_source_and_sink|{\"converter\":\"org.apache.kafka.connect.json.JsonConverter\",\"schemas\":true,\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "connect-rest-compatible|tests/kafkatest/tests/connect/connect_rest_test.py::ConnectRestApiTest.test_rest_api|{\"connect_protocol\":\"compatible\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "connect-distributed-file|tests/kafkatest/tests/connect/connect_distributed_test.py::ConnectDistributedTest.test_file_source_and_sink|{\"security_protocol\":\"PLAINTEXT\",\"exactly_once_source\":false,\"connect_protocol\":\"compatible\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "connect-distributed-exactly-once-source|tests/kafkatest/tests/connect/connect_distributed_test.py::ConnectDistributedTest.test_exactly_once_source|{\"clean\":true,\"connect_protocol\":\"sessioned\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "streams-smoke|tests/kafkatest/tests/streams/streams_smoke_test.py::StreamsSmokeTest.test_streams|{\"processing_guarantee\":\"at_least_once\",\"crash\":false,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"enable_assignment_batching\":true,\"transactional\":false}"
    "broker-bounce|tests/kafkatest/sanity_checks/test_bounce.py::TestBounce.test_simple_run|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"quorum_size\":1}"
    "performance-clients|tests/kafkatest/sanity_checks/test_performance_services.py::PerformanceServiceTest.test_version|{\"version\":\"dev\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "produce-bench|tests/kafkatest/tests/core/produce_bench_test.py::ProduceBenchTest.test_produce_bench|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "produce-bench-transactions|tests/kafkatest/tests/core/produce_bench_test.py::ProduceBenchTest.test_produce_bench_transactions|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "consume-bench-single|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_single_partition|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consume-bench-topics-classic|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_consume_bench|{\"topics\":[\"consume_bench_topic[0-5]\"],\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consume-bench-topics-next-gen|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_consume_bench|{\"topics\":[\"consume_bench_topic[0-5]\"],\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\"}"
    "consume-bench-manual-partitions|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_consume_bench|{\"topics\":[\"consume_bench_topic[0-5]:[0-4]\"],\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consume-bench-random-groups|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_multiple_consumers_random_group_topics|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\"}"
    "consume-bench-same-group|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_two_consumers_specified_group_topics|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\"}"
    "consume-bench-random-partitions|tests/kafkatest/tests/core/consume_bench_test.py::ConsumeBenchTest.test_multiple_consumers_random_group_partitions|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "round-trip-workload|tests/kafkatest/tests/core/round_trip_fault_test.py::RoundTripFaultTest.test_round_trip_workload|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "produce-consume|tests/kafkatest/tests/client/client_compatibility_produce_consume_test.py::ClientCompatibilityProduceConsumeTest.test_produce_consume|{\"broker_version\":\"dev\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "old-clients-2.1-zstd|tests/kafkatest/tests/core/compatibility_test_new_broker_test.py::ClientCompatibilityTestNewBroker.test_compatibility|{\"producer_version\":\"2.1.1\",\"consumer_version\":\"2.1.1\",\"compression_types\":[\"zstd\"],\"timestamp_type\":\"CreateTime\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "old-clients-2.8|tests/kafkatest/tests/core/compatibility_test_new_broker_test.py::ClientCompatibilityTestNewBroker.test_compatibility|{\"producer_version\":\"2.8.2\",\"consumer_version\":\"2.8.2\",\"compression_types\":[\"none\"],\"timestamp_type\":\"CreateTime\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "old-clients-3.9|tests/kafkatest/tests/core/compatibility_test_new_broker_test.py::ClientCompatibilityTestNewBroker.test_compatibility|{\"producer_version\":\"3.9.2\",\"consumer_version\":\"3.9.2\",\"compression_types\":[\"none\"],\"timestamp_type\":\"CreateTime\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "old-clients-4.3|tests/kafkatest/tests/core/compatibility_test_new_broker_test.py::ClientCompatibilityTestNewBroker.test_compatibility|{\"producer_version\":\"4.3.1\",\"consumer_version\":\"4.3.1\",\"compression_types\":[\"none\"],\"timestamp_type\":\"CreateTime\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "consumer-groups-list-classic|tests/kafkatest/tests/core/consumer_group_command_test.py::ConsumerGroupCommandTest.test_list_consumer_groups|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consumer-groups-list-next-gen|tests/kafkatest/tests/core/consumer_group_command_test.py::ConsumerGroupCommandTest.test_list_consumer_groups|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\"}"
    "consumer-groups-describe-classic|tests/kafkatest/tests/core/consumer_group_command_test.py::ConsumerGroupCommandTest.test_describe_consumer_group|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consumer-groups-describe-next-gen|tests/kafkatest/tests/core/consumer_group_command_test.py::ConsumerGroupCommandTest.test_describe_consumer_group|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\"}"
    "share-groups-list|tests/kafkatest/tests/core/share_group_command_test.py::ShareGroupCommandTest.test_list_share_groups|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-groups-describe|tests/kafkatest/tests/core/share_group_command_test.py::ShareGroupCommandTest.test_describe_share_group|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-groups-describe-members|tests/kafkatest/tests/core/share_group_command_test.py::ShareGroupCommandTest.test_describe_share_group_members|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "consumer-group-consumption|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_group_consumption|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\",\"enable_assignment_batching\":true}"
    "replica-verification|tests/kafkatest/tests/tools/replica_verification_test.py::ReplicaVerificationToolTest.test_replica_lags|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "consumer-group-consumption-classic|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_group_consumption|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"enable_assignment_batching\":true}"
    "consumer-assignment-classic|tests/kafkatest/tests/client/consumer_test.py::AssignmentValidationTest.test_valid_assignment|{\"assignment_strategy\":\"org.apache.kafka.clients.consumer.RangeAssignor\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "consumer-assignment-next-gen|tests/kafkatest/tests/client/consumer_test.py::AssignmentValidationTest.test_valid_assignment|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\",\"group_remote_assignor\":\"range\",\"enable_assignment_batching\":true}"
    "consumer-failure-classic|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_failure|{\"clean_shutdown\":true,\"enable_autocommit\":true,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"enable_assignment_batching\":true}"
    "consumer-failure-next-gen|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_failure|{\"clean_shutdown\":true,\"enable_autocommit\":true,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\",\"enable_assignment_batching\":true}"
    "consumer-bounce-classic|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_bounce|{\"clean_shutdown\":true,\"bounce_mode\":\"rolling\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"enable_assignment_batching\":true}"
    "consumer-bounce-next-gen|tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_consumer_bounce|{\"clean_shutdown\":true,\"bounce_mode\":\"rolling\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"consumer\",\"enable_assignment_batching\":true}"
    "replication-clean-shutdown|tests/kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure|{\"failure_mode\":\"clean_shutdown\",\"security_protocol\":\"PLAINTEXT\",\"broker_type\":\"leader\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "replication-hard-shutdown|tests/kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure|{\"failure_mode\":\"hard_shutdown\",\"security_protocol\":\"PLAINTEXT\",\"broker_type\":\"leader\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "replication-clean-bounce|tests/kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure|{\"failure_mode\":\"clean_bounce\",\"security_protocol\":\"PLAINTEXT\",\"broker_type\":\"leader\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "replication-idempotent|tests/kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure|{\"failure_mode\":\"clean_shutdown\",\"security_protocol\":\"PLAINTEXT\",\"broker_type\":\"leader\",\"enable_idempotence\":true,\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "replication-gzip|tests/kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure|{\"failure_mode\":\"clean_shutdown\",\"security_protocol\":\"PLAINTEXT\",\"broker_type\":\"leader\",\"compression_type\":\"gzip\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "partition-reassignment|tests/kafkatest/tests/core/reassign_partitions_test.py::ReassignPartitionsTest.test_reassign_partitions|{\"bounce_brokers\":false,\"reassign_from_offset_zero\":true,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\"}"
    "controller-mutation-quota|tests/kafkatest/tests/core/controller_mutation_quota_test.py::ControllerMutationQuotaTest.test_controller_mutation_quota|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "transactions|tests/kafkatest/tests/core/transactions_test.py::TransactionsTest.test_transactions|{\"failure_mode\":\"clean_bounce\",\"bounce_target\":\"clients\",\"check_order\":true,\"use_group_metadata\":false,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"use_transactions_v2\":false}"
    "transactions-v2|tests/kafkatest/tests/core/transactions_test.py::TransactionsTest.test_transactions|{\"failure_mode\":\"clean_bounce\",\"bounce_target\":\"clients\",\"check_order\":true,\"use_group_metadata\":false,\"metadata_quorum\":\"COMBINED_KRAFT\",\"group_protocol\":\"classic\",\"use_transactions_v2\":true}"
    "group-mode-transactions|tests/kafkatest/tests/core/group_mode_transactions_test.py::GroupModeTransactionsTest.test_transactions|{\"failure_mode\":\"clean_bounce\",\"bounce_target\":\"clients\",\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-consumer-single-partition|tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_share_single_topic_partition|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"enable_assignment_batching\":true}"
    "share-consumer-multiple-partitions|tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_share_multiple_partitions|{\"metadata_quorum\":\"COMBINED_KRAFT\",\"enable_assignment_batching\":true}"
    "share-consumer-bounce|tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_share_consumer_bounce|{\"clean_shutdown\":true,\"bounce_mode\":\"rolling\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"enable_assignment_batching\":true}"
    "share-consumer-failure|tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_share_consumer_failure|{\"clean_shutdown\":true,\"num_failed_consumers\":1,\"metadata_quorum\":\"COMBINED_KRAFT\",\"enable_assignment_batching\":true}"
    "share-broker-failure|tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_broker_failure|{\"clean_shutdown\":true,\"num_failed_brokers\":1,\"metadata_quorum\":\"COMBINED_KRAFT\",\"enable_assignment_batching\":true}"
    "share-consume-bench|tests/kafkatest/tests/core/share_consume_bench_test.py::ShareConsumeBenchTest.test_share_consume_bench|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-consume-bench-two-consumers|tests/kafkatest/tests/core/share_consume_bench_test.py::ShareConsumeBenchTest.test_two_share_consumers_in_a_group_topics|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-consume-bench-single-topic|tests/kafkatest/tests/core/share_consume_bench_test.py::ShareConsumeBenchTest.test_one_share_consumer_subscribed_to_single_topic|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "share-consume-bench-single-partition-contention|tests/kafkatest/tests/core/share_consume_bench_test.py::ShareConsumeBenchTest.test_multiple_share_consumers_subscribed_to_single_topic|{\"metadata_quorum\":\"COMBINED_KRAFT\"}"
    "log-compaction|tests/kafkatest/tests/tools/log_compaction_test.py::LogCompactionTest.test_log_compaction|{\"security_protocol\":\"PLAINTEXT\",\"metadata_quorum\":\"COMBINED_KRAFT\",\"compression_config\":{}}"
)

if (( $# > 0 )); then
    selected=()
    for requested in "$@"; do
        found=false
        for entry in "${tests[@]}"; do
            IFS='|' read -r name _ _ <<< "$entry"
            if [[ "$name" == "$requested" ]]; then
                selected+=("$entry")
                found=true
                break
            fi
        done
        if [[ "$found" == false ]]; then
            echo "Unknown conformance test: $requested" >&2
            exit 2
        fi
    done
    tests=("${selected[@]}")
fi

mapfile -t kafka_service_files < <(
    rg -l '^from kafkatest\.services\.kafka import .*KafkaService' \
        "$kafka_dir/tests/kafkatest/tests" \
        "$kafka_dir/tests/kafkatest/sanity_checks"
)
for path in "${kafka_service_files[@]}"; do
    sed -i \
        -e 's/from kafkatest.services.kafka import KafkaService, config_property, quorum, consumer_group/from kafkatest.services.kafka import config_property, quorum, consumer_group\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import KafkaService, quorum, consumer_group, TopicPartition/from kafkatest.services.kafka import quorum, consumer_group, TopicPartition\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import KafkaService, quorum, consumer_group/from kafkatest.services.kafka import quorum, consumer_group\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import KafkaService, quorum/from kafkatest.services.kafka import quorum\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import config_property, KafkaService, quorum/from kafkatest.services.kafka import config_property, quorum\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import config_property, KafkaService/from kafkatest.services.kafka import config_property\nfrom kafkatest.services.crabka import CrabkaService as KafkaService/' \
        -e 's/from kafkatest.services.kafka import KafkaService$/from kafkatest.services.crabka import CrabkaService as KafkaService/' \
        "$path"
done
cp "$repo_root/scripts/kafka-conformance/crabka.py" \
    "$kafka_dir/tests/kafkatest/services/crabka.py"
# Kafka supports custom JDK base images, but its Dockerfile still names Jammy's
# virtual netcat package and old pip. Keep the Noble base compatible with the
# glibc used to build Crabka.
sed -i \
    -e 's/sudo git netcat iptables/sudo git netcat-openbsd iptables/' \
    -e 's/RUN python3 -m pip install -U pip==21.1.1;/RUN true/' \
    -e 's/RUN pip3 install --break-system-packages --upgrade/RUN pip3 install --break-system-packages --ignore-installed --upgrade/' \
    -e 's/RUN pip3 install --upgrade/RUN pip3 install --break-system-packages --ignore-installed --upgrade/' \
    -e 's/curl -s /curl --fail --silent --show-error --retry 5 /g' \
    "$kafka_dir/tests/docker/Dockerfile"

(cd "$kafka_dir" && ./gradlew systemTestLibs --no-daemon)
(cd "$kafka_dir" && tests/docker/ducker-ak up -n 11 -j docker.io/library/eclipse-temurin:17-jdk-noble)
trap '(cd "$kafka_dir" && tests/docker/ducker-ak down -f)' EXIT

printf '# Kafka conformance\n\nKafka `%s`\n\n| Test | Result |\n|---|---|\n' "$kafka_ref" > "$report"
passed=0
for entry in "${tests[@]}"; do
    IFS='|' read -r name path parameters <<< "$entry"
    log="${report_dir}/${name}.log"
    (cd "$kafka_dir" && tests/docker/ducker-ak test --skip-build "$path" -- --parameters "'$parameters'") \
        2>&1 | tee "$log" || true
    if grep -Eq '^passed:[[:space:]]+1$' "$log" && grep -Eq '^failed:[[:space:]]+0$' "$log"; then
        result=PASS
        passed=$((passed + 1))
    else
        result=FAIL
    fi
    printf '| `%s` | **%s** |\n' "$name" "$result" >> "$report"
done

printf '\n**%d/%d passed**\n' "$passed" "${#tests[@]}" >> "$report"
cat "$report"
[[ "$passed" -eq "${#tests[@]}" ]]

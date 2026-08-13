"""Ducktape KafkaService adapter that starts Crabka brokers."""

import signal

from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService


class CrabkaService(KafkaService):
    DATA_DIR = "/mnt/kafka/crabka-data"
    CONFIG_FILE = "/mnt/kafka/crabka.toml"
    CONTROLLER_PORT = 9093
    CLUSTER_ID = "00000000-0000-0000-0000-000000000001"

    def start(self, **kwargs):
        if len(self.nodes) > 1:
            self.concurrent_start = True
        return super().start(**kwargs)

    def start_node(self, node, timeout_sec=60, **kwargs):
        if self.security_protocol != "PLAINTEXT":
            raise RuntimeError("the Crabka Ducktape adapter currently supports PLAINTEXT only")

        node.account.mkdirs(self.PERSISTENT_ROOT)
        node_id = self.idx(node)
        client_port = self.port_mappings["PLAINTEXT"].port_number
        voters = [
            f'{self.idx(peer)}@{peer.account.hostname}:{self.CONTROLLER_PORT}'
            for peer in self.nodes
        ]
        server_props = dict(self.server_prop_overrides)
        segment_bytes = server_props.get("log.segment.bytes")
        runtime = "\n[runtime]\n"
        runtime += 'classic_group_initial_rebalance_delay = "100ms"\n'
        quota_windows = server_props.get("quota.window.num")
        controller_window = server_props.get("controller.quota.window.size.seconds")
        if quota_windows and controller_window:
            runtime += (
                'controller_mutation_quota_window = '
                f'"{int(quota_windows) * int(controller_window)}s"\n'
            )
        if segment_bytes:
            runtime += f'log_segment_bytes = "{segment_bytes}B"\n'
        config = f'''broker_id = {node_id}
log_dir = "{self.DATA_DIR}"
inter_broker_listener_name = "PLAINTEXT"
controller_quorum_voters = [{", ".join(f'"{voter}"' for voter in voters)}]

[[listeners]]
name = "PLAINTEXT"
bind_addr = "0.0.0.0:{client_port}"
advertised = "{node.account.hostname}:{client_port}"
protocol = "Plaintext"

[process]
roles = ["broker", "controller"]
{runtime}
'''
        node.account.create_file(self.CONFIG_FILE, config)
        node.account.ssh(
            f'test -f {self.DATA_DIR}/meta.properties.json || '
            f'/opt/kafka-dev/crabka format --log-dir {self.DATA_DIR} '
            f'--cluster-id {self.CLUSTER_ID} --node-id {node_id} '
            f'--feature transaction.version={2 if self.use_transactions_v2 else 0}'
        )
        offsets = (self.topics or {}).get("__consumer_offsets", {})
        offsets_args = (
            f' --offsets-topic-num-partitions {offsets.get("partitions", 50)}'
            f' --offsets-topic-replication-factor {offsets.get("replication-factor", 3)}'
            if offsets else ""
        )
        cmd = (
            f'/opt/kafka-dev/crabka-broker --broker-id {node_id} '
            f'--config-file {self.CONFIG_FILE} '
            f'--cluster-id {self.CLUSTER_ID} --metrics-listen-addr none '
            f'{offsets_args} '
            f'1>> {self.STDOUT_STDERR_CAPTURE} 2>&1 &'
        )
        if self.concurrent_start:
            node.account.ssh(cmd)
        else:
            with node.account.monitor_log(self.STDOUT_STDERR_CAPTURE) as monitor:
                node.account.ssh(cmd)
                self.wait_for_start(node, monitor, timeout_sec)
        if not self.pids(node):
            raise RuntimeError(f"Crabka broker exited on {node.account.hostname}")

    def pids(self, node):
        return [
            int(line.strip())
            for line in node.account.ssh_capture("pgrep -f '[c]rabka-broker' || true")
            if line.strip().isdigit()
        ]

    def stop_node(self, node, clean_shutdown=True, timeout_sec=60):
        self.signal_node(node, signal.SIGTERM if clean_shutdown else signal.SIGKILL)
        wait_until(
            lambda: not self.pids(node),
            timeout_sec=timeout_sec,
            err_msg=f"Crabka broker failed to stop in {timeout_sec} seconds",
        )

    def create_topic(self, topic_cfg, node=None):
        if topic_cfg["topic"] == "__consumer_offsets":
            return
        return super().create_topic(topic_cfg, node)

    def wait_for_start(self, node, monitor, timeout_sec=60):
        monitor.wait_until("crabka-broker listening", timeout_sec=timeout_sec)

    def thread_dump(self, node):
        pass

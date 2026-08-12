#include "crabka/client.hpp"
#include "crabka/errors.hpp"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace {
std::vector<std::uint8_t> bytes(const std::string& value) { return {value.begin(), value.end()}; }

void expect_error(const crabka::ErrorKind kind, const std::string& message,
                  const auto& operation) {
  try {
    operation();
    assert(false);
  } catch (const crabka::SdkError& error) {
    assert(error.error().kind == kind);
    assert(error.error().message == message);
  }
}
} // namespace

int main() {
  crabka::Client client;
  (void)client.publish(crabka::Record{
      .topic = "jobs",
      .value = bytes("one"),
      .headers = {{"kind", bytes("queue")}, {"nullable", std::nullopt}},
  });
  (void)client.publish(
      crabka::Record{.topic = "jobs", .value = bytes("two"), .headers = {}});
  (void)client.publish(
      crabka::Record{.topic = "other", .value = bytes("skip"), .headers = {}});

  const auto first = client.queue_acquire("jobs", "workers", 1, 30'000);
  assert(first.session_id == "queue-session-1");
  assert(first.messages.size() == 1);
  assert(first.messages[0].message_id == "jobs:0:0");
  assert(first.messages[0].value == bytes("one"));
  assert(first.messages[0].headers.size() == 2);
  assert(!first.messages[0].headers[1].value.has_value());
  assert(first.messages[0].delivery_count == 1);

  const auto renewed = client.queue_renew(
      first.session_id, {{.message_id = "jobs:0:0"}, {.message_id = "missing:0:0"}});
  assert(renewed.results.size() == 2);
  assert(!renewed.results[0].error.has_value());
  assert(renewed.results[1].error.has_value());
  assert(renewed.results[1].error->kind == crabka::ErrorKind::InvalidArgument);
  assert(renewed.results[1].error->message == "queue message is not acquired");

  const auto released = client.queue_acknowledge(
      first.session_id,
      {{.message_id = "jobs:0:0", .ack_type = crabka::QueueAckType::Release}});
  assert(released.results.size() == 1);
  assert(!released.results[0].error.has_value());

  const auto redelivered = client.queue_acquire("jobs", "workers", 1, 30'000, first.session_id);
  assert(redelivered.session_id == first.session_id);
  assert(redelivered.messages.size() == 1);
  assert(redelivered.messages[0].message_id == "jobs:0:0");
  assert(redelivered.messages[0].delivery_count == 2);

  const auto accepted = client.queue_acknowledge(
      redelivered.session_id,
      {{.message_id = "jobs:0:0", .ack_type = crabka::QueueAckType::Accept},
       {.message_id = "missing:0:0", .ack_type = crabka::QueueAckType::Reject}});
  assert(accepted.results.size() == 2);
  assert(!accepted.results[0].error.has_value());
  assert(accepted.results[1].error.has_value());

  const auto second = client.queue_acquire("jobs", "workers", 2, 30'000);
  assert(second.session_id == "queue-session-2");
  assert(second.messages.size() == 1);
  assert(second.messages[0].message_id == "jobs:0:1");

  expect_error(crabka::ErrorKind::InvalidArgument, "queue group is required", [&] {
    (void)client.queue_acquire("jobs", "", 1, 30'000);
  });
  expect_error(
      crabka::ErrorKind::InvalidArgument,
      "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported", [&] {
        (void)client.queue_acquire("jobs", "workers", 1, 1'000);
      });
  expect_error(crabka::ErrorKind::InvalidArgument, "queue session_id is required", [&] {
    (void)client.queue_acknowledge("", {});
  });
  expect_error(crabka::ErrorKind::InvalidArgument, "queue session_id is required", [&] {
    (void)client.queue_renew("", {});
  });

  crabka::Client isolated;
  (void)isolated.publish(
      crabka::Record{.topic = "jobs", .value = bytes("one"), .headers = {}});
  const auto owner = isolated.queue_acquire("jobs", "workers", 1, 30'000);
  const auto other = isolated.queue_acquire("jobs", "workers", 1, 30'000);
  const auto wrong_ack = isolated.queue_acknowledge(
      other.session_id,
      {{.message_id = owner.messages[0].message_id, .ack_type = crabka::QueueAckType::Accept}});
  assert(wrong_ack.results[0].error.has_value());
  assert(wrong_ack.results[0].error->message == "queue message is not acquired");
  const auto wrong_renew = isolated.queue_renew(
      other.session_id, {{.message_id = owner.messages[0].message_id}});
  assert(wrong_renew.results[0].error.has_value());
  assert(wrong_renew.results[0].error->message == "queue message is not acquired");
  expect_error(crabka::ErrorKind::InvalidArgument, "queue session expired; re-acquire", [&] {
    (void)isolated.queue_acquire("jobs", "workers", 1, 30'000, "missing-session");
  });
  expect_error(
      crabka::ErrorKind::InvalidArgument,
      "group_id and topics are fixed when a queue session is created", [&] {
        (void)isolated.queue_acquire("jobs", "other-workers", 1, 30'000,
                                     owner.session_id);
      });

  crabka::Client independent_groups;
  (void)independent_groups.publish(
      crabka::Record{.topic = "jobs", .value = bytes("one"), .headers = {}});
  const auto group_a = independent_groups.queue_acquire("jobs", "workers-a", 1, 30'000);
  const auto group_b = independent_groups.queue_acquire("jobs", "workers-b", 1, 30'000);
  assert(group_a.messages.size() == 1);
  assert(group_b.messages.size() == 1);
  assert(group_a.messages[0].delivery_count == 1);
  assert(group_b.messages[0].delivery_count == 1);
  const auto group_b_accepted = independent_groups.queue_acknowledge(
      group_b.session_id,
      {{.message_id = group_b.messages[0].message_id,
        .ack_type = crabka::QueueAckType::Accept}});
  assert(!group_b_accepted.results[0].error.has_value());
  const auto group_a_renewed = independent_groups.queue_renew(
      group_a.session_id, {{.message_id = group_a.messages[0].message_id}});
  assert(!group_a_renewed.results[0].error.has_value());
  const auto group_a_accepted = independent_groups.queue_acknowledge(
      group_a.session_id,
      {{.message_id = group_a.messages[0].message_id,
        .ack_type = crabka::QueueAckType::Accept}});
  assert(!group_a_accepted.results[0].error.has_value());

  client.configure("mock://gateway", std::nullopt);
  const auto after_reset = client.queue_acquire("jobs", "workers", 1, 30'000);
  assert(after_reset.session_id == "queue-session-1");
  assert(after_reset.messages.empty());

  crabka::Client unreachable("unreachable://gateway");
  expect_error(crabka::ErrorKind::Transport, "endpoint unreachable", [&] {
    (void)unreachable.queue_acquire("jobs", "workers", 1, 30'000);
  });

  return 0;
}

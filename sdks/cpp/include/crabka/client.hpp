#pragma once

#include "crabka/errors.hpp"
#include "crabka/json.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace crabka {

struct Header {
  std::string name;
  std::optional<std::vector<std::uint8_t>> value;
};

struct PublishResult {
  std::int32_t partition = 0;
  std::int64_t offset = 0;
  bool deduplicated = false;
};

struct Record {
  std::string topic;
  std::vector<std::uint8_t> value;
  std::vector<Header> headers;
};

struct CloudEvent {
  std::string id;
  std::string source;
  std::string type;
  std::string specversion;
  std::optional<std::string> datacontenttype;
  std::vector<std::uint8_t> data;
};

struct Filter {
  std::string path;
  std::string op;
  json::Value value;
};

struct Inbound {
  std::string topic;
  std::int32_t partition = 0;
  std::int64_t offset = 0;
  std::vector<std::uint8_t> value;
  std::vector<Header> headers;
};

enum class QueueAckType {
  Accept,
  Release,
  Reject,
};

struct QueueAckEntry {
  std::string message_id;
  QueueAckType ack_type = QueueAckType::Accept;
};

struct QueueRenewEntry {
  std::string message_id;
};

struct QueueOperationError {
  ErrorKind kind;
  std::string message;
};

struct QueueResult {
  std::string message_id;
  std::optional<QueueOperationError> error;
};

struct QueueMessage {
  std::string message_id;
  std::string topic;
  std::int32_t partition = 0;
  std::int64_t offset = 0;
  std::vector<std::uint8_t> value;
  std::vector<Header> headers;
  std::int32_t delivery_count = 0;
};

struct QueueAcquireResult {
  std::string session_id;
  std::vector<QueueMessage> messages;
};

struct QueueBatchResult {
  std::vector<QueueResult> results;
};

struct LiveTransportHeaderForTest {
  std::string name;
  std::string value;
};

struct LiveTransportRequestForTest {
  std::string path;
  std::vector<LiveTransportHeaderForTest> headers;
  std::vector<std::uint8_t> body;
  bool end_stream_after_body = true;
};

struct LiveTransportRequestBodyReadPlanForTest {
  bool final_body_read_ends_stream = false;
  bool read_after_body_defers = false;
  bool close_after_body_ends_stream = false;
  bool close_before_body_drained_copies_body = false;
  bool close_before_body_drained_ends_stream = false;
  bool close_before_body_drained_ends_stream_after_body = false;
};

struct LiveTransportStreamClosePlanForTest {
  bool close_marks_request_body_closed = false;
  bool close_keeps_stream_owner = false;
  bool protocol_close_removes_stream_owner = false;
  bool reader_state_survives_protocol_close = false;
};

struct LiveTransportSendSafetyForTest {
  bool uses_msg_nosignal = false;
  bool suppresses_sigpipe = false;
};

struct LiveTransportResponseLifecycleForTest {
  bool success_status_allows_response = false;
  bool bad_status_fails_after_close = false;
  bool data_before_close_is_preserved = false;
  bool end_stream_closes_stream = false;
  std::optional<Error> connect_http_error;
  std::optional<Error> connect_end_stream_error;
};

class LiveSubscription {
public:
  virtual ~LiveSubscription() = default;
  [[nodiscard]] virtual Inbound next(std::uint64_t timeout_ms) = 0;
  virtual void close() = 0;
};

class MessageStream {
public:
  MessageStream() = default;
  explicit MessageStream(std::vector<Inbound> messages);
  explicit MessageStream(std::shared_ptr<LiveSubscription> subscription);
  [[nodiscard]] Inbound next(std::uint64_t timeout_ms);
  void close();

private:
  std::vector<Inbound> messages_;
  std::size_t cursor_ = 0;
  std::shared_ptr<LiveSubscription> subscription_;
};

class Client {
public:
  explicit Client(std::string endpoint = "mock://gateway");
  void configure(std::string endpoint, std::optional<std::string> bearer);
  [[nodiscard]] bool bearer_configured() const noexcept;
  [[nodiscard]] PublishResult publish(Record record);
  [[nodiscard]] PublishResult publish_event(std::string topic, CloudEvent event);
  [[nodiscard]] MessageStream subscribe(const std::vector<std::string>& topics,
                                        const std::string& group, const std::optional<Filter>& filter);
  [[nodiscard]] QueueAcquireResult queue_acquire(const std::string& topic, const std::string& group,
                                                 std::uint32_t max, std::uint64_t lock_duration_ms,
                                                 const std::string& session_id = "");
  [[nodiscard]] QueueBatchResult queue_acknowledge(const std::string& session_id,
                                                   const std::vector<QueueAckEntry>& entries);
  [[nodiscard]] QueueBatchResult queue_renew(const std::string& session_id,
                                             const std::vector<QueueRenewEntry>& entries);
  [[nodiscard]] Error queue_error() const;
  [[nodiscard]] Error database_error() const;
  [[nodiscard]] Error auth_error() const;
  [[nodiscard]] Error blob_error() const;

private:
  std::string endpoint_;
  std::optional<std::string> bearer_;
  std::vector<Record> records_;
  enum class MockQueueState { Available, Acquired, Accepted, Rejected };
  struct MockQueueDeliveryState {
    MockQueueState state = MockQueueState::Available;
    std::string session_id;
    std::int32_t delivery_count = 0;
  };
  std::vector<std::map<std::string, MockQueueDeliveryState>> queue_states_;
  struct MockQueueSession {
    std::string topic;
    std::string group;
    std::uint32_t max_messages;
  };
  std::map<std::string, MockQueueSession> queue_sessions_;
  std::uint64_t next_queue_session_id_ = 1;

  [[nodiscard]] bool is_unreachable() const;
};

[[nodiscard]] Error live_transport_unavailable_error();
[[nodiscard]] PublishResult live_transport_publish(const std::string& endpoint,
                                                   const std::optional<std::string>& bearer,
                                                   const Record& record);
[[nodiscard]] MessageStream live_transport_subscribe(const std::string& endpoint,
                                                     const std::optional<std::string>& bearer,
                                                     const std::vector<std::string>& topics,
                                                     const std::string& group,
                                                     const std::optional<Filter>& filter);
[[nodiscard]] QueueAcquireResult live_transport_queue_acquire(
    const std::string& endpoint, const std::optional<std::string>& bearer, const std::string& topic,
    const std::string& group, std::uint32_t max, std::uint64_t lock_duration_ms,
    const std::string& session_id);
[[nodiscard]] QueueBatchResult live_transport_queue_acknowledge(
    const std::string& endpoint, const std::optional<std::string>& bearer,
    const std::string& session_id, const std::vector<QueueAckEntry>& entries);
[[nodiscard]] QueueBatchResult live_transport_queue_renew(
    const std::string& endpoint, const std::optional<std::string>& bearer,
    const std::string& session_id, const std::vector<QueueRenewEntry>& entries);
[[nodiscard]] std::vector<std::uint8_t> live_transport_send_request_bytes_for_test(const Record& record);
[[nodiscard]] LiveTransportRequestForTest live_transport_send_http_request_for_test(
    const Record& record, const std::optional<std::string>& bearer);
[[nodiscard]] std::vector<std::uint8_t> live_transport_subscribe_start_bytes_for_test(
    const std::vector<std::string>& topics, const std::string& group, const std::optional<Filter>& filter);
[[nodiscard]] LiveTransportRequestForTest live_transport_subscribe_http_request_for_test(
    const std::vector<std::string>& topics, const std::string& group,
    const std::optional<Filter>& filter, const std::optional<std::string>& bearer);
[[nodiscard]] LiveTransportRequestForTest live_transport_queue_acquire_http_request_for_test(
    const std::string& topic, const std::string& group, std::uint32_t max,
    std::uint64_t lock_duration_ms, const std::string& session_id,
    const std::optional<std::string>& bearer);
[[nodiscard]] LiveTransportRequestForTest live_transport_queue_acknowledge_http_request_for_test(
    const std::string& session_id, const std::vector<QueueAckEntry>& entries,
    const std::optional<std::string>& bearer);
[[nodiscard]] LiveTransportRequestForTest live_transport_queue_renew_http_request_for_test(
    const std::string& session_id, const std::vector<QueueRenewEntry>& entries,
    const std::optional<std::string>& bearer);
[[nodiscard]] QueueAcquireResult live_transport_queue_acquire_response_for_test(
    const std::vector<std::uint8_t>& response);
[[nodiscard]] QueueBatchResult live_transport_queue_acknowledge_response_for_test(
    const std::vector<std::uint8_t>& response, const std::vector<QueueAckEntry>& entries);
[[nodiscard]] QueueBatchResult live_transport_queue_renew_response_for_test(
    const std::vector<std::uint8_t>& response, const std::vector<QueueRenewEntry>& entries);
[[nodiscard]] LiveTransportRequestBodyReadPlanForTest live_transport_request_body_read_plan_for_test(
    const LiveTransportRequestForTest& request);
[[nodiscard]] LiveTransportStreamClosePlanForTest live_transport_stream_close_plan_for_test();
[[nodiscard]] LiveTransportSendSafetyForTest live_transport_send_safety_for_test();
[[nodiscard]] LiveTransportResponseLifecycleForTest live_transport_response_lifecycle_for_test();

} // namespace crabka

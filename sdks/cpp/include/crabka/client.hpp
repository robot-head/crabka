#pragma once

#include "crabka/errors.hpp"
#include "crabka/json.hpp"

#include <cstdint>
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
  [[nodiscard]] Error queue_error() const;
  [[nodiscard]] Error database_error() const;
  [[nodiscard]] Error auth_error() const;
  [[nodiscard]] Error blob_error() const;

private:
  std::string endpoint_;
  std::optional<std::string> bearer_;
  std::vector<Record> records_;

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
[[nodiscard]] std::vector<std::uint8_t> live_transport_send_request_bytes_for_test(const Record& record);
[[nodiscard]] LiveTransportRequestForTest live_transport_send_http_request_for_test(
    const Record& record, const std::optional<std::string>& bearer);
[[nodiscard]] std::vector<std::uint8_t> live_transport_subscribe_start_bytes_for_test(
    const std::vector<std::string>& topics, const std::string& group, const std::optional<Filter>& filter);
[[nodiscard]] LiveTransportRequestForTest live_transport_subscribe_http_request_for_test(
    const std::vector<std::string>& topics, const std::string& group,
    const std::optional<Filter>& filter, const std::optional<std::string>& bearer);
[[nodiscard]] LiveTransportRequestBodyReadPlanForTest live_transport_request_body_read_plan_for_test(
    const LiveTransportRequestForTest& request);
[[nodiscard]] LiveTransportStreamClosePlanForTest live_transport_stream_close_plan_for_test();
[[nodiscard]] LiveTransportSendSafetyForTest live_transport_send_safety_for_test();
[[nodiscard]] LiveTransportResponseLifecycleForTest live_transport_response_lifecycle_for_test();

} // namespace crabka

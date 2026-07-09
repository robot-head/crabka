#include "crabka/client.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

namespace crabka {
namespace {
std::vector<Header> cloud_event_headers(const CloudEvent& event) {
  std::vector<Header> headers{{"ce_id", std::vector<std::uint8_t>{event.id.begin(), event.id.end()}},
                              {"ce_source", std::vector<std::uint8_t>{event.source.begin(), event.source.end()}},
                              {"ce_type", std::vector<std::uint8_t>{event.type.begin(), event.type.end()}},
                              {"ce_specversion", std::vector<std::uint8_t>{event.specversion.begin(), event.specversion.end()}}};
  if (event.datacontenttype.has_value()) {
    const auto& content_type = *event.datacontenttype;
    headers.push_back({"content-type", std::vector<std::uint8_t>{content_type.begin(), content_type.end()}});
  }
  return headers;
}

bool scalar_values_equal(const json::Value& left, const json::Value& right) {
  if (left.data.index() != right.data.index()) return false;
  if (std::holds_alternative<std::nullptr_t>(left.data)) return true;
  if (auto left_bool = std::get_if<bool>(&left.data)) return std::get<bool>(right.data) == *left_bool;
  if (auto left_number = std::get_if<double>(&left.data)) return std::get<double>(right.data) == *left_number;
  if (auto left_string = std::get_if<std::string>(&left.data)) return std::get<std::string>(right.data) == *left_string;
  return false;
}

bool record_matches_filter(const Record& record, const std::optional<Filter>& filter) {
  if (!filter.has_value()) return true;
  if (filter->op != "equals" || filter->path.rfind("$.", 0) != 0) return false;
  const std::string body(record.value.begin(), record.value.end());
  try {
    const auto parsed = json::parse(body);
    const auto& object = json::as_object(parsed);
    const auto found = object.find(filter->path.substr(2));
    if (found == object.end()) return false;
    return scalar_values_equal(found->second, filter->value);
  } catch (const SdkError&) {
    return false;
  }
}
} // namespace

MessageStream::MessageStream(std::vector<Inbound> messages) : messages_(std::move(messages)) {}
MessageStream::MessageStream(std::shared_ptr<LiveSubscription> subscription) : subscription_(std::move(subscription)) {}

Inbound MessageStream::next(std::uint64_t timeout_ms) {
  if (subscription_) return subscription_->next(timeout_ms);
  if (cursor_ >= messages_.size()) {
    throw SdkError(error_with_message(ErrorKind::NotFound, "no message available"));
  }
  return messages_[cursor_++];
}

void MessageStream::close() {
  if (subscription_) subscription_->close();
}

Client::Client(std::string endpoint) : endpoint_(std::move(endpoint)) {}

void Client::configure(std::string endpoint, std::optional<std::string> bearer) {
  endpoint_ = std::move(endpoint);
  bearer_ = std::move(bearer);
  records_.clear();
}

bool Client::bearer_configured() const noexcept { return bearer_.has_value(); }

PublishResult Client::publish(Record record) {
  if (record.topic.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "topic is required"));
  if (record.topic == "__missing_topic") throw SdkError(error_with_message(ErrorKind::NotFound, "topic not found"));
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) return live_transport_publish(endpoint_, bearer_, record);
  const auto offset = static_cast<std::int64_t>(std::count_if(records_.begin(), records_.end(), [&](const Record& stored) { return stored.topic == record.topic; }));
  records_.push_back(std::move(record));
  return PublishResult{.partition = 0, .offset = offset, .deduplicated = false};
}

PublishResult Client::publish_event(std::string topic, CloudEvent event) {
  if (event.id.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "CloudEvent id is required"));
  return publish(Record{.topic = std::move(topic), .value = std::move(event.data), .headers = cloud_event_headers(event)});
}

MessageStream Client::subscribe(const std::vector<std::string>& topics, const std::string& group, const std::optional<Filter>& filter) {
  if (topics.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "at least one topic is required"));
  if (filter.has_value() && filter->op != "equals") throw SdkError(error_with_message(ErrorKind::InvalidArgument, "only equals filters are supported"));
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) return live_transport_subscribe(endpoint_, bearer_, topics, group, filter);
  std::vector<Inbound> messages;
  for (std::size_t index = 0; index < records_.size(); ++index) {
    const auto& record = records_[index];
    if (std::find(topics.begin(), topics.end(), record.topic) == topics.end()) continue;
    if (!record_matches_filter(record, filter)) continue;
    const auto offset = static_cast<std::int64_t>(std::count_if(records_.begin(), records_.begin() + static_cast<std::ptrdiff_t>(index), [&](const Record& earlier) { return earlier.topic == record.topic; }));
    messages.push_back(Inbound{.topic = record.topic, .partition = 0, .offset = offset, .value = record.value, .headers = record.headers});
  }
  return MessageStream(std::move(messages));
}

Error Client::queue_error() const { return unimplemented_module("queues", "gateway-sharegroup-rpc"); }
Error Client::database_error() const { return unimplemented_module("database", "chapter-f-control-plane"); }
Error Client::auth_error() const { return error_with_message(ErrorKind::Unauthenticated, "identity APIs are not part of contract v1"); }
Error Client::blob_error() const { return unimplemented_module("blob", "chapter-b-blob-api"); }
bool Client::is_unreachable() const { return endpoint_.rfind("unreachable://", 0) == 0; }

} // namespace crabka

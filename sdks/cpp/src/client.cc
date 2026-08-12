#include "crabka/client.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
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

void validate_filter(const std::optional<Filter>& filter) {
  if (!filter.has_value()) return;
  if (filter->op != "equals") {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "only equals filters are supported"));
  }
  if (filter->path.rfind("$.", 0) != 0) {
    throw SdkError(error_with_message(
        ErrorKind::InvalidArgument, "filter path must start with $."));
  }
  const auto field = filter->path.substr(2);
  const auto valid_segment = [](std::string_view segment) {
    if (segment.empty() || !(segment.front() == '_' ||
                             (segment.front() >= 'A' && segment.front() <= 'Z') ||
                             (segment.front() >= 'a' && segment.front() <= 'z'))) {
      return false;
    }
    return std::all_of(segment.begin() + 1, segment.end(), [](char character) {
      return character == '_' || (character >= 'A' && character <= 'Z') ||
             (character >= 'a' && character <= 'z') ||
             (character >= '0' && character <= '9');
    });
  };
  std::size_t begin = 0;
  while (begin <= field.size()) {
    const auto end = field.find('.', begin);
    if (!valid_segment(field.substr(begin, end - begin))) {
      throw SdkError(error_with_message(
          ErrorKind::InvalidArgument, "filter path must contain identifier segments"));
    }
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  if (std::holds_alternative<json::Array>(filter->value.data) ||
      std::holds_alternative<json::Object>(filter->value.data)) {
    throw SdkError(error_with_message(
        ErrorKind::InvalidArgument, "filter value must be a string, number, boolean, or null"));
  }
}

std::int64_t record_offset(const std::vector<Record>& records, std::size_t index) {
  return static_cast<std::int64_t>(std::count_if(
      records.begin(), records.begin() + static_cast<std::ptrdiff_t>(index),
      [&](const Record& earlier) { return earlier.topic == records[index].topic; }));
}

std::string queue_message_id(const std::vector<Record>& records, std::size_t index) {
  return records[index].topic + ":0:" + std::to_string(record_offset(records, index));
}

QueueResult queue_not_acquired(std::string message_id) {
  return QueueResult{.message_id = std::move(message_id),
                     .error = QueueOperationError{
                         .kind = ErrorKind::InvalidArgument,
                         .message = "queue message is not acquired"}};
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
  queue_states_.clear();
  queue_sessions_.clear();
  next_queue_session_id_ = 1;
}

bool Client::bearer_configured() const noexcept { return bearer_.has_value(); }

PublishResult Client::publish(Record record) {
  if (record.topic.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "topic is required"));
  if (record.topic == "__missing_topic") throw SdkError(error_with_message(ErrorKind::NotFound, "topic not found"));
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) return live_transport_publish(endpoint_, bearer_, record);
  const auto offset = static_cast<std::int64_t>(std::count_if(records_.begin(), records_.end(), [&](const Record& stored) { return stored.topic == record.topic; }));
  records_.push_back(std::move(record));
  queue_states_.emplace_back();
  return PublishResult{.partition = 0, .offset = offset, .deduplicated = false};
}

PublishResult Client::publish_event(std::string topic, CloudEvent event) {
  if (event.id.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "CloudEvent id is required"));
  return publish(Record{.topic = std::move(topic), .value = std::move(event.data), .headers = cloud_event_headers(event)});
}

MessageStream Client::subscribe(const std::vector<std::string>& topics, const std::string& group, const std::optional<Filter>& filter) {
  if (topics.empty()) throw SdkError(error_with_message(ErrorKind::InvalidArgument, "at least one topic is required"));
  validate_filter(filter);
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) return live_transport_subscribe(endpoint_, bearer_, topics, group, filter);
  std::vector<Inbound> messages;
  for (std::size_t index = 0; index < records_.size(); ++index) {
    const auto& record = records_[index];
    if (std::find(topics.begin(), topics.end(), record.topic) == topics.end()) continue;
    if (!record_matches_filter(record, filter)) continue;
    const auto offset = record_offset(records_, index);
    messages.push_back(Inbound{.topic = record.topic, .partition = 0, .offset = offset, .value = record.value, .headers = record.headers});
  }
  return MessageStream(std::move(messages));
}

QueueAcquireResult Client::queue_acquire(const std::string& topic, const std::string& group,
                                         std::uint32_t max, std::uint64_t lock_duration_ms,
                                         const std::string& session_id) {
  if (topic.empty()) {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "queue topic is required"));
  }
  if (group.empty()) {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "queue group is required"));
  }
  if (lock_duration_ms != 30'000) {
    throw SdkError(error_with_message(
        ErrorKind::InvalidArgument,
        "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported"));
  }
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) {
    return live_transport_queue_acquire(endpoint_, bearer_, topic, group, max, lock_duration_ms,
                                        session_id);
  }

  const auto effective_max = std::clamp(max, std::uint32_t{1}, std::uint32_t{500});
  std::string actual_session_id = session_id;
  if (actual_session_id.empty()) {
    actual_session_id = "queue-session-" + std::to_string(next_queue_session_id_++);
    queue_sessions_.emplace(actual_session_id, MockQueueSession{.topic = topic, .group = group,
                                                                .max_messages = effective_max});
  } else {
    const auto session = queue_sessions_.find(actual_session_id);
    if (session == queue_sessions_.end()) {
      throw SdkError(error_with_message(
          ErrorKind::InvalidArgument, "queue session expired; re-acquire"));
    }
    if (session->second.topic != topic || session->second.group != group) {
      throw SdkError(error_with_message(
          ErrorKind::InvalidArgument,
          "group_id and topics are fixed when a queue session is created"));
    }
    if (max != 0 && effective_max != session->second.max_messages) {
      throw SdkError(error_with_message(
          ErrorKind::InvalidArgument, "max_messages is fixed when a queue session is created"));
    }
  }

  QueueAcquireResult result{.session_id = actual_session_id, .messages = {}};
  result.messages.reserve(effective_max);
  for (std::size_t index = 0;
       index < records_.size() && result.messages.size() < effective_max; ++index) {
    auto& state = queue_states_[index][group];
    if (records_[index].topic != topic || state.state != MockQueueState::Available) continue;
    state.state = MockQueueState::Acquired;
    state.session_id = actual_session_id;
    if (state.delivery_count < std::numeric_limits<std::int32_t>::max()) ++state.delivery_count;
    const auto offset = record_offset(records_, index);
    result.messages.push_back(QueueMessage{.message_id = queue_message_id(records_, index),
                                           .topic = records_[index].topic,
                                           .partition = 0,
                                           .offset = offset,
                                           .value = records_[index].value,
                                           .headers = records_[index].headers,
                                           .delivery_count = state.delivery_count});
  }
  return result;
}

QueueBatchResult Client::queue_acknowledge(const std::string& session_id,
                                           const std::vector<QueueAckEntry>& entries) {
  if (session_id.empty()) {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "queue session_id is required"));
  }
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) {
    return live_transport_queue_acknowledge(endpoint_, bearer_, session_id, entries);
  }
  if (!queue_sessions_.contains(session_id)) {
    throw SdkError(error_with_message(
        ErrorKind::InvalidArgument, "queue session expired; re-acquire"));
  }
  const auto& group = queue_sessions_.at(session_id).group;

  QueueBatchResult batch;
  batch.results.reserve(entries.size());
  for (const auto& entry : entries) {
    auto index = records_.size();
    for (std::size_t candidate = 0; candidate < records_.size(); ++candidate) {
      const auto state = queue_states_[candidate].find(group);
      if (state != queue_states_[candidate].end() &&
          state->second.state == MockQueueState::Acquired &&
          state->second.session_id == session_id && queue_message_id(records_, candidate) == entry.message_id) {
        index = candidate;
        break;
      }
    }
    if (index == records_.size()) {
      batch.results.push_back(queue_not_acquired(entry.message_id));
      continue;
    }
    auto& state = queue_states_[index].at(group);
    if (entry.ack_type == QueueAckType::Release) {
      state.state = MockQueueState::Available;
    } else if (entry.ack_type == QueueAckType::Reject) {
      state.state = MockQueueState::Rejected;
    } else {
      state.state = MockQueueState::Accepted;
    }
    state.session_id.clear();
    batch.results.push_back(QueueResult{.message_id = entry.message_id, .error = std::nullopt});
  }
  return batch;
}

QueueBatchResult Client::queue_renew(const std::string& session_id,
                                     const std::vector<QueueRenewEntry>& entries) {
  if (session_id.empty()) {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "queue session_id is required"));
  }
  if (is_unreachable()) throw SdkError(error_with_message(ErrorKind::Transport, "endpoint unreachable"));
  if (endpoint_.rfind("mock://", 0) != 0) {
    return live_transport_queue_renew(endpoint_, bearer_, session_id, entries);
  }
  if (!queue_sessions_.contains(session_id)) {
    throw SdkError(error_with_message(
        ErrorKind::InvalidArgument, "queue session expired; re-acquire"));
  }
  const auto& group = queue_sessions_.at(session_id).group;

  QueueBatchResult batch;
  batch.results.reserve(entries.size());
  for (const auto& entry : entries) {
    bool acquired = false;
    for (std::size_t index = 0; index < records_.size(); ++index) {
      const auto state = queue_states_[index].find(group);
      if (state != queue_states_[index].end() &&
          state->second.state == MockQueueState::Acquired &&
          state->second.session_id == session_id && queue_message_id(records_, index) == entry.message_id) {
        acquired = true;
        break;
      }
    }
    if (acquired) {
      batch.results.push_back(QueueResult{.message_id = entry.message_id, .error = std::nullopt});
    } else {
      batch.results.push_back(queue_not_acquired(entry.message_id));
    }
  }
  return batch;
}

Error Client::queue_error() const { return unimplemented_module("queues", "gateway-sharegroup-rpc"); }
Error Client::database_error() const { return unimplemented_module("database", "chapter-f-control-plane"); }
Error Client::auth_error() const { return error_with_message(ErrorKind::Unauthenticated, "identity APIs are not part of contract v1"); }
Error Client::blob_error() const { return unimplemented_module("blob", "chapter-b-blob-api"); }
bool Client::is_unreachable() const { return endpoint_.rfind("unreachable://", 0) == 0; }

} // namespace crabka

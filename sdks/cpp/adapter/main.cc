#include "crabka/base64.hpp"
#include "crabka/client.hpp"
#include "crabka/json.hpp"

#include <iostream>
#include <optional>

namespace {
using crabka::json::Array;
using crabka::json::Object;
using crabka::json::Value;

constexpr double kContractMajor = 1;
constexpr double kContractMinor = 0;

Value string_value(const std::string& value) { return Value{value}; }
Value null_value() { return Value{nullptr}; }
Value number_value(double value) { return Value{value}; }
Value bool_value(bool value) { return Value{value}; }
Value object_value(Object value) { return Value{std::move(value)}; }
Value array_value(Array value) { return Value{std::move(value)}; }

Object ok(Object body) { return Object{{"ok", object_value(std::move(body))}}; }

Object error_response(const crabka::Error& error) {
  Object body{{"kind", string_value(crabka::to_string(error.kind))}};
  if (!error.module.empty()) body.emplace("module", string_value(error.module));
  if (!error.gated_on.empty()) body.emplace("gated_on", string_value(error.gated_on));
  if (!error.message.empty()) body.emplace("message", string_value(error.message));
  return Object{{"error", object_value(std::move(body))}};
}

std::vector<crabka::Header> parse_headers(const Object& command) {
  std::vector<crabka::Header> headers;
  auto found = command.find("headers");
  if (found == command.end()) return headers;
  for (const auto& value : crabka::json::as_array(found->second)) {
    const auto& object = crabka::json::as_object(value);
    std::optional<std::vector<std::uint8_t>> header_value;
    if (crabka::json::has_non_null(object, "value_b64")) {
      header_value = crabka::base64_decode(crabka::json::get_string(object, "value_b64"));
    }
    headers.push_back({crabka::json::get_string(object, "name"), std::move(header_value)});
  }
  return headers;
}

Array headers_json(const std::vector<crabka::Header>& headers) {
  Array out;
  for (const auto& header : headers) {
    out.push_back(object_value(Object{{"name", string_value(header.name)}, {"value_b64", header.value.has_value() ? string_value(crabka::base64_encode(*header.value)) : null_value()}}));
  }
  return out;
}

std::optional<crabka::Filter> parse_filter(const Object& command) {
  auto found = command.find("filter");
  if (found == command.end() || std::holds_alternative<std::nullptr_t>(found->second.data)) return std::nullopt;
  const auto& object = crabka::json::as_object(found->second);
  return crabka::Filter{.path = crabka::json::get_string(object, "path"), .op = crabka::json::get_string(object, "op"), .value = object.at("value")};
}

std::vector<std::string> parse_topics(const Object& command) {
  std::vector<std::string> topics;
  for (const auto& topic : crabka::json::as_array(command.at("topics"))) topics.push_back(crabka::json::as_string(topic));
  return topics;
}

Object publish_response(const crabka::PublishResult& result) {
  return ok(Object{{"deduplicated", bool_value(result.deduplicated)}, {"offset", number_value(static_cast<double>(result.offset))}, {"partition", number_value(static_cast<double>(result.partition))}});
}

class Adapter {
public:
  Object handle(const Object& command) {
    const std::string cmd = crabka::json::get_string(command, "cmd");
    try {
      if (cmd == "hello") {
        return Object{{"hello",
                       object_value(Object{{"contract_major", number_value(kContractMajor)},
                                           {"contract_minor", number_value(kContractMinor)},
                                           {"language", string_value("cpp")}})}};
      }
      if (cmd == "configure") {
        std::optional<std::string> bearer;
        if (crabka::json::has_non_null(command, "bearer")) bearer = crabka::json::get_string(command, "bearer");
        client_.configure(crabka::json::get_string(command, "endpoint"), bearer);
        stream_ = std::nullopt;
        return ok(Object{{"bearer_configured", bool_value(client_.bearer_configured())}});
      }
      if (cmd == "publish") {
        auto value = crabka::base64_decode(crabka::json::get_string(command, "value_b64"));
        return publish_response(client_.publish(crabka::Record{.topic = crabka::json::get_string(command, "topic"), .value = std::move(value), .headers = parse_headers(command)}));
      }
      if (cmd == "publish_event") {
        const auto& event_object = crabka::json::as_object(command.at("event"));
        crabka::CloudEvent event{.id = crabka::json::get_string(event_object, "id"), .source = crabka::json::get_string(event_object, "source"), .type = crabka::json::get_string(event_object, "type"), .specversion = crabka::json::get_string(event_object, "specversion"), .datacontenttype = std::nullopt, .data = crabka::base64_decode(crabka::json::get_string(event_object, "data_b64"))};
        if (crabka::json::has_non_null(event_object, "datacontenttype")) event.datacontenttype = crabka::json::get_string(event_object, "datacontenttype");
        return publish_response(client_.publish_event(crabka::json::get_string(command, "topic"), std::move(event)));
      }
      if (cmd == "subscribe") {
        stream_ = client_.subscribe(parse_topics(command), crabka::json::get_string(command, "group"), parse_filter(command));
        return ok(Object{});
      }
      if (cmd == "next_message") {
        if (!stream_.has_value()) return error_response(crabka::error_with_message(crabka::ErrorKind::InvalidArgument, "subscribe before next_message"));
        auto message = stream_->next(static_cast<std::uint64_t>(std::get<double>(command.at("timeout_ms").data)));
        return Object{{"message", object_value(Object{{"headers", array_value(headers_json(message.headers))}, {"offset", number_value(static_cast<double>(message.offset))}, {"partition", number_value(static_cast<double>(message.partition))}, {"topic", string_value(message.topic)}, {"value_b64", string_value(crabka::base64_encode(message.value))}})}};
      }
      if (cmd == "queue_acquire" || cmd == "queue_ack") return error_response(client_.queue_error());
      if (cmd == "db_connect") return error_response(client_.database_error());
      if (cmd == "auth_sign_in") return error_response(client_.auth_error());
      if (cmd == "blob_put" || cmd == "blob_get") return error_response(client_.blob_error());
      return error_response(crabka::error_with_message(crabka::ErrorKind::InvalidArgument, "unknown command"));
    } catch (const crabka::SdkError& error) {
      return error_response(error.error());
    }
  }

private:
  crabka::Client client_;
  std::optional<crabka::MessageStream> stream_;
};
} // namespace

int main() {
  Adapter adapter;
  std::string line;
  while (std::getline(std::cin, line)) {
    const auto command = crabka::json::as_object(crabka::json::parse(line));
    std::cout << crabka::json::stringify(object_value(adapter.handle(command))) << '\n' << std::flush;
  }
  return 0;
}

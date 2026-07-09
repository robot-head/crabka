#include "crabka/client.hpp"
#include "crabka/errors.hpp"
#include "transport_nghttp2_parse.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/socket.h>
#endif

namespace {
std::vector<std::uint8_t> bytes(const std::string& value) { return {value.begin(), value.end()}; }

std::optional<std::string> header_value(const crabka::LiveTransportRequestForTest& request,
                                        const std::string& name) {
  for (const auto& header : request.headers) {
    if (header.name == name) return header.value;
  }
  return std::nullopt;
}

bool has_connect_streaming_envelope(const std::vector<std::uint8_t>& body) {
  if (body.size() < 5) return false;
  if (body[0] != 0) return false;
  const auto size = (static_cast<std::uint32_t>(body[1]) << 24U) |
                    (static_cast<std::uint32_t>(body[2]) << 16U) |
                    (static_cast<std::uint32_t>(body[3]) << 8U) |
                    static_cast<std::uint32_t>(body[4]);
  return static_cast<std::size_t>(size) == body.size() - 5;
}

std::vector<std::uint8_t> connect_message(const std::vector<std::uint8_t>& payload) {
  const auto size = static_cast<std::uint32_t>(payload.size());
  std::vector<std::uint8_t> out{0,
                                static_cast<std::uint8_t>((size >> 24U) & 255U),
                                static_cast<std::uint8_t>((size >> 16U) & 255U),
                                static_cast<std::uint8_t>((size >> 8U) & 255U),
                                static_cast<std::uint8_t>(size & 255U)};
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}
} // namespace

int main() {
  const crabka::Record record{.topic = "t", .value = bytes("v"), .headers = {{"h", bytes("x")}}};
  const auto send_request = crabka::live_transport_send_request_bytes_for_test(record);
  assert(send_request == std::vector<std::uint8_t>({0x0a, 0x0e, 0x0a, 0x01, 't', 0x1a, 0x01, 'v',
                                                    0x22, 0x06, 0x0a, 0x01, 'h', 0x12, 0x01, 'x'}));
  const auto send_http = crabka::live_transport_send_http_request_for_test(record, "token");
  assert(send_http.path == "/crabka.gateway.v1.Gateway/Send");
  assert(header_value(send_http, "content-type") == "application/proto");
  assert(header_value(send_http, "authorization") == "Bearer token");
  assert(!header_value(send_http, "connect-protocol-version").has_value());
  assert(send_http.body == send_request);
  assert(send_http.end_stream_after_body);
  assert(!has_connect_streaming_envelope(send_http.body));
  const auto send_body_plan = crabka::live_transport_request_body_read_plan_for_test(send_http);
  assert(send_body_plan.final_body_read_ends_stream);
  assert(!send_body_plan.read_after_body_defers);
  assert(send_body_plan.close_before_body_drained_copies_body);
  assert(!send_body_plan.close_before_body_drained_ends_stream);
  assert(send_body_plan.close_before_body_drained_ends_stream_after_body);

  const auto subscribe_start = crabka::live_transport_subscribe_start_bytes_for_test({"t"}, "g", std::nullopt);
  assert(subscribe_start == std::vector<std::uint8_t>({0x0a, 0x08, 0x0a, 0x01, 'g', 0x12, 0x01, 't', 0x18, 0x01}));
  const auto subscribe_http = crabka::live_transport_subscribe_http_request_for_test({"t"}, "g", std::nullopt, std::nullopt);
  assert(subscribe_http.path == "/crabka.gateway.v1.Gateway/Subscribe");
  assert(header_value(subscribe_http, "content-type") == "application/connect+proto");
  assert(header_value(subscribe_http, "connect-protocol-version") == "1");
  assert(header_value(subscribe_http, "te") == "trailers");
  assert(subscribe_http.body == connect_message(subscribe_start));
  assert(!subscribe_http.end_stream_after_body);
  assert(has_connect_streaming_envelope(subscribe_http.body));
  const auto subscribe_body_plan = crabka::live_transport_request_body_read_plan_for_test(subscribe_http);
  assert(!subscribe_body_plan.final_body_read_ends_stream);
  assert(subscribe_body_plan.read_after_body_defers);
  assert(subscribe_body_plan.close_after_body_ends_stream);
  assert(subscribe_body_plan.close_before_body_drained_copies_body);
  assert(!subscribe_body_plan.close_before_body_drained_ends_stream);
  assert(subscribe_body_plan.close_before_body_drained_ends_stream_after_body);

  const auto stream_close_plan = crabka::live_transport_stream_close_plan_for_test();
  assert(stream_close_plan.close_marks_request_body_closed);
  assert(stream_close_plan.close_keeps_stream_owner);
  assert(stream_close_plan.protocol_close_removes_stream_owner);
  assert(stream_close_plan.reader_state_survives_protocol_close);

  const auto send_safety = crabka::live_transport_send_safety_for_test();
  assert(send_safety.suppresses_sigpipe);
#ifdef MSG_NOSIGNAL
  assert(send_safety.uses_msg_nosignal);
#endif

  assert(!crabka::detail::http2_parse_failure_message(0, "parse error").has_value());
  assert(!crabka::detail::http2_parse_failure_message(42, "parse error").has_value());
  const auto parse_failure = crabka::detail::http2_parse_failure_message(-902, "parse error");
  assert(parse_failure.has_value());
  assert(parse_failure->find("parse HTTP/2 data failed") == 0);

  const auto response_lifecycle = crabka::live_transport_response_lifecycle_for_test();
  assert(response_lifecycle.success_status_allows_response);
  assert(response_lifecycle.bad_status_fails_after_close);
  assert(response_lifecycle.data_before_close_is_preserved);
  assert(response_lifecycle.end_stream_closes_stream);

  crabka::Client client;
  const auto published = client.publish(crabka::Record{.topic = "t", .value = bytes("v"), .headers = {}});
  assert(published.offset == 0);
  auto stream = client.subscribe({"t"}, "g", std::nullopt);
  assert(stream.next(0).value == bytes("v"));
  stream.close();

  crabka::Client unreachable("unreachable://gateway");
  try {
    (void)unreachable.publish(crabka::Record{.topic = "t", .value = bytes("v"), .headers = {}});
    assert(false);
  } catch (const crabka::SdkError& error) {
    assert(error.error().kind == crabka::ErrorKind::Transport);
  }
  try {
    (void)unreachable.subscribe({"t"}, "g", std::nullopt);
    assert(false);
  } catch (const crabka::SdkError& error) {
    assert(error.error().kind == crabka::ErrorKind::Transport);
  }

  return 0;
}

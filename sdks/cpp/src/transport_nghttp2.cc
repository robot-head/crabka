#include "crabka/client.hpp"

#include "crabka/envelope.hpp"
#include "crabka/json.hpp"
#include "transport_nghttp2_parse.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/socket.h>
#endif

#ifdef CRABKA_CPP_HAS_NGHTTP2
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <nghttp2/nghttp2.h>
#include <poll.h>
#include <unistd.h>
#endif

namespace crabka {
namespace {
constexpr std::string_view kSendPath = "/crabka.gateway.v1.Gateway/Send";
constexpr std::string_view kSubscribePath = "/crabka.gateway.v1.Gateway/Subscribe";
constexpr std::string_view kUnaryContentType = "application/proto";
constexpr std::string_view kStreamingContentType = "application/connect+proto";
constexpr std::string_view kConnectProtocolVersion = "1";
constexpr std::uint8_t kMessageFlags = 0;

int sigpipe_safe_send_flags() {
#ifdef MSG_NOSIGNAL
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}

[[maybe_unused]] int nonblocking_receive_flags() {
#ifdef MSG_DONTWAIT
  return MSG_DONTWAIT;
#else
  return 0;
#endif
}

bool transport_writes_suppress_sigpipe() {
#if defined(MSG_NOSIGNAL) || defined(SO_NOSIGPIPE) || defined(SIGPIPE)
  return true;
#else
  return false;
#endif
}

struct Endpoint {
  std::string host;
  std::string port;
  std::string authority;
};

struct FieldReader {
  std::string_view bytes;
  std::size_t cursor = 0;
};

struct RequestBodyState {
  std::vector<std::uint8_t> bytes;
  std::size_t cursor = 0;
  bool end_stream_after_body = true;
  bool close_requested = false;
};

struct RequestBodyReadOutcome {
  std::size_t copied = 0;
  bool deferred = false;
  bool end_stream = false;
};

void append_varint(std::vector<std::uint8_t>& out, std::uint64_t value) {
  while (value >= 0x80U) {
    out.push_back(static_cast<std::uint8_t>(value | 0x80U));
    value >>= 7U;
  }
  out.push_back(static_cast<std::uint8_t>(value));
}

void append_key(std::vector<std::uint8_t>& out, std::uint32_t field, std::uint8_t wire_type) {
  append_varint(out, (static_cast<std::uint64_t>(field) << 3U) | wire_type);
}

void append_int(std::vector<std::uint8_t>& out, std::uint32_t field, std::uint64_t value) {
  append_key(out, field, 0);
  append_varint(out, value);
}

void append_bytes(std::vector<std::uint8_t>& out, std::uint32_t field, std::string_view bytes) {
  append_key(out, field, 2);
  append_varint(out, bytes.size());
  out.insert(out.end(), bytes.begin(), bytes.end());
}

void append_bytes(std::vector<std::uint8_t>& out, std::uint32_t field, const std::vector<std::uint8_t>& bytes) {
  append_key(out, field, 2);
  append_varint(out, bytes.size());
  out.insert(out.end(), bytes.begin(), bytes.end());
}

std::uint64_t read_varint(FieldReader& reader) {
  std::uint64_t value = 0;
  for (std::uint32_t shift = 0; shift < 64; shift += 7) {
    if (reader.cursor >= reader.bytes.size()) throw SdkError(error_with_message(ErrorKind::Transport, "truncated protobuf varint"));
    const auto byte = static_cast<std::uint8_t>(reader.bytes[reader.cursor++]);
    value |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0U) return value;
  }
  throw SdkError(error_with_message(ErrorKind::Transport, "protobuf varint is too long"));
}

std::string_view read_length_delimited(FieldReader& reader) {
  const auto size = static_cast<std::size_t>(read_varint(reader));
  if (reader.bytes.size() - reader.cursor < size) throw SdkError(error_with_message(ErrorKind::Transport, "truncated protobuf field"));
  const auto start = reader.cursor;
  reader.cursor += size;
  return reader.bytes.substr(start, size);
}

void skip_field(FieldReader& reader, std::uint8_t wire_type) {
  if (wire_type == 0) {
    (void)read_varint(reader);
    return;
  }
  if (wire_type == 2) {
    (void)read_length_delimited(reader);
    return;
  }
  throw SdkError(error_with_message(ErrorKind::Transport, "unsupported protobuf wire type"));
}

std::string gateway_filter_literal(const json::Value& value);

std::string filter_expression(const std::optional<Filter>& filter) {
  if (!filter.has_value()) return {};
  if (filter->path.rfind("$.", 0) != 0) return {};
  return filter->path.substr(2) + " = " + gateway_filter_literal(filter->value);
}

std::string quote_gateway_filter_string(std::string_view value) {
  std::string quoted;
  quoted.reserve(value.size() + 2);
  quoted.push_back('\'');
  for (const auto character : value) {
    if (character == '\\' || character == '\'') quoted.push_back('\\');
    quoted.push_back(character);
  }
  quoted.push_back('\'');
  return quoted;
}

std::string gateway_filter_literal(const json::Value& value) {
  if (const auto* string_value = std::get_if<std::string>(&value.data); string_value != nullptr) {
    return quote_gateway_filter_string(*string_value);
  }
  if (const auto* bool_value = std::get_if<bool>(&value.data); bool_value != nullptr) {
    return *bool_value ? "true" : "false";
  }
  if (std::holds_alternative<double>(value.data)) return json::stringify(value);
  return "null";
}

RequestBodyState request_body_state(std::vector<std::uint8_t> bytes, bool end_stream_after_body) {
  return RequestBodyState{.bytes = std::move(bytes),
                          .cursor = 0,
                          .end_stream_after_body = end_stream_after_body,
                          .close_requested = false};
}

RequestBodyReadOutcome read_drained_request_body(const RequestBodyState& state) {
  if (state.end_stream_after_body || state.close_requested) {
    return RequestBodyReadOutcome{.copied = 0, .deferred = false, .end_stream = true};
  }
  return RequestBodyReadOutcome{.copied = 0, .deferred = true, .end_stream = false};
}

RequestBodyReadOutcome read_request_body_chunk(RequestBodyState& state, std::uint8_t* buffer,
                                               std::size_t length) {
  const auto remaining = state.bytes.size() - state.cursor;
  if (remaining == 0) return read_drained_request_body(state);
  if (length == 0) return RequestBodyReadOutcome{.copied = 0, .deferred = true, .end_stream = false};

  const auto copied = std::min(length, remaining);
  std::memcpy(buffer, state.bytes.data() + state.cursor, copied);
  state.cursor += copied;
  const auto body_drained = state.cursor == state.bytes.size();
  return RequestBodyReadOutcome{.copied = copied,
                                .deferred = false,
                                .end_stream = body_drained && (state.end_stream_after_body || state.close_requested)};
}

void append_http_header(std::vector<LiveTransportHeaderForTest>& headers, std::string_view name,
                        std::string_view value) {
  headers.push_back({std::string(name), std::string(value)});
}

void append_authorization_header(std::vector<LiveTransportHeaderForTest>& headers,
                                 const std::optional<std::string>& bearer) {
  if (!bearer.has_value()) return;
  append_http_header(headers, "authorization", "Bearer " + *bearer);
}

std::vector<std::uint8_t> encode_connect_message(const std::vector<std::uint8_t>& payload) {
  std::vector<std::uint8_t> out;
  out.reserve(payload.size() + 5);
  out.push_back(kMessageFlags);
  const auto size = static_cast<std::uint32_t>(payload.size());
  out.push_back(static_cast<std::uint8_t>((size >> 24U) & 255U));
  out.push_back(static_cast<std::uint8_t>((size >> 16U) & 255U));
  out.push_back(static_cast<std::uint8_t>((size >> 8U) & 255U));
  out.push_back(static_cast<std::uint8_t>(size & 255U));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

LiveTransportRequestForTest make_unary_http_request(std::string_view path, std::vector<std::uint8_t> body,
                                                     const std::optional<std::string>& bearer) {
  LiveTransportRequestForTest request{.path = std::string(path),
                                      .headers = {},
                                      .body = std::move(body),
                                      .end_stream_after_body = true};
  append_http_header(request.headers, "content-type", kUnaryContentType);
  append_authorization_header(request.headers, bearer);
  return request;
}

LiveTransportRequestForTest make_streaming_http_request(std::string_view path, std::vector<std::uint8_t> payload,
                                                        const std::optional<std::string>& bearer) {
  LiveTransportRequestForTest request{.path = std::string(path),
                                      .headers = {},
                                      .body = encode_connect_message(payload),
                                      .end_stream_after_body = false};
  append_http_header(request.headers, "content-type", kStreamingContentType);
  append_http_header(request.headers, "connect-protocol-version", kConnectProtocolVersion);
  append_http_header(request.headers, "te", "trailers");
  append_authorization_header(request.headers, bearer);
  return request;
}

[[maybe_unused]] Endpoint parse_endpoint(const std::string& endpoint) {
  constexpr std::string_view http_prefix = "http://";
  if (endpoint.rfind(http_prefix, 0) != 0) throw SdkError(error_with_message(ErrorKind::Transport, "live transport requires an h2c http:// endpoint"));
  const auto authority = endpoint.substr(http_prefix.size());
  const auto slash = authority.find('/');
  if (slash != std::string::npos) throw SdkError(error_with_message(ErrorKind::Transport, "live transport endpoint must not include a path"));
  const auto colon = authority.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 == authority.size()) {
    throw SdkError(error_with_message(ErrorKind::Transport, "live transport endpoint must include host:port"));
  }
  return Endpoint{.host = authority.substr(0, colon), .port = authority.substr(colon + 1), .authority = authority};
}

std::vector<std::uint8_t> encode_header(const Header& header) {
  std::vector<std::uint8_t> out;
  append_bytes(out, 1, header.name);
  if (header.value.has_value()) append_bytes(out, 2, *header.value);
  return out;
}

std::vector<std::uint8_t> encode_record(const Record& record) {
  std::vector<std::uint8_t> out;
  append_bytes(out, 1, record.topic);
  append_bytes(out, 3, record.value);
  for (const auto& header : record.headers) append_bytes(out, 4, encode_header(header));
  return out;
}

std::vector<std::uint8_t> encode_send_request(const Record& record) {
  std::vector<std::uint8_t> out;
  append_bytes(out, 1, encode_record(record));
  return out;
}

std::vector<std::uint8_t> encode_subscribe_start(const std::vector<std::string>& topics, const std::string& group,
                                                 const std::optional<Filter>& filter) {
  std::vector<std::uint8_t> start;
  append_bytes(start, 1, group);
  for (const auto& topic : topics) append_bytes(start, 2, topic);
  append_int(start, 3, 1);
  const auto expression = filter_expression(filter);
  if (!expression.empty()) append_bytes(start, 4, expression);

  std::vector<std::uint8_t> frame;
  append_bytes(frame, 1, start);
  return frame;
}

LiveTransportRequestForTest make_send_http_request(const Record& record,
                                                    const std::optional<std::string>& bearer) {
  return make_unary_http_request(kSendPath, encode_send_request(record), bearer);
}

LiveTransportRequestForTest make_subscribe_http_request(const std::vector<std::string>& topics,
                                                         const std::string& group,
                                                         const std::optional<Filter>& filter,
                                                         const std::optional<std::string>& bearer) {
  return make_streaming_http_request(kSubscribePath, encode_subscribe_start(topics, group, filter), bearer);
}

std::vector<Header> decode_headers(std::string_view bytes) {
  FieldReader reader{.bytes = bytes};
  std::vector<Header> headers;
  Header header;
  while (reader.cursor < reader.bytes.size()) {
    const auto key = read_varint(reader);
    const auto field = static_cast<std::uint32_t>(key >> 3U);
    const auto wire_type = static_cast<std::uint8_t>(key & 0x07U);
    if (field == 1 && wire_type == 2) {
      const auto value = read_length_delimited(reader);
      header.name.assign(value.begin(), value.end());
      continue;
    }
    if (field == 2 && wire_type == 2) {
      const auto value = read_length_delimited(reader);
      header.value = std::vector<std::uint8_t>(value.begin(), value.end());
      continue;
    }
    skip_field(reader, wire_type);
  }
  if (!header.name.empty()) headers.push_back(std::move(header));
  return headers;
}

[[maybe_unused]] PublishResult decode_send_response(const std::vector<std::uint8_t>& bytes) {
  FieldReader reader{.bytes = std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
  while (reader.cursor < reader.bytes.size()) {
    const auto key = read_varint(reader);
    const auto field = static_cast<std::uint32_t>(key >> 3U);
    const auto wire_type = static_cast<std::uint8_t>(key & 0x07U);
    if (field != 1 || wire_type != 2) {
      skip_field(reader, wire_type);
      continue;
    }
    FieldReader result_reader{.bytes = read_length_delimited(reader)};
    PublishResult result;
    while (result_reader.cursor < result_reader.bytes.size()) {
      const auto result_key = read_varint(result_reader);
      const auto result_field = static_cast<std::uint32_t>(result_key >> 3U);
      const auto result_wire_type = static_cast<std::uint8_t>(result_key & 0x07U);
      if (result_field == 1 && result_wire_type == 0) {
        result.partition = static_cast<std::int32_t>(read_varint(result_reader));
      } else if (result_field == 2 && result_wire_type == 0) {
        result.offset = static_cast<std::int64_t>(read_varint(result_reader));
      } else if (result_field == 3 && result_wire_type == 0) {
        result.deduplicated = read_varint(result_reader) != 0;
      } else if (result_field == 4 && result_wire_type == 2) {
        throw SdkError(error_with_message(ErrorKind::Transport, "gateway rejected record"));
      } else {
        skip_field(result_reader, result_wire_type);
      }
    }
    return result;
  }
  throw SdkError(error_with_message(ErrorKind::Transport, "Send response did not include a result"));
}

[[maybe_unused]] Inbound decode_inbound(const std::vector<std::uint8_t>& bytes) {
  FieldReader reader{.bytes = std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
  Inbound inbound;
  while (reader.cursor < reader.bytes.size()) {
    const auto key = read_varint(reader);
    const auto field = static_cast<std::uint32_t>(key >> 3U);
    const auto wire_type = static_cast<std::uint8_t>(key & 0x07U);
    if (field == 1 && wire_type == 2) {
      const auto value = read_length_delimited(reader);
      inbound.topic.assign(value.begin(), value.end());
    } else if (field == 2 && wire_type == 0) {
      inbound.partition = static_cast<std::int32_t>(read_varint(reader));
    } else if (field == 3 && wire_type == 0) {
      inbound.offset = static_cast<std::int64_t>(read_varint(reader));
    } else if (field == 5 && wire_type == 2) {
      const auto value = read_length_delimited(reader);
      inbound.value.assign(value.begin(), value.end());
    } else if (field == 6 && wire_type == 2) {
      auto headers = decode_headers(read_length_delimited(reader));
      inbound.headers.insert(inbound.headers.end(), std::make_move_iterator(headers.begin()), std::make_move_iterator(headers.end()));
    } else {
      skip_field(reader, wire_type);
    }
  }
  return inbound;
}

struct StreamState {
  RequestBodyState request_body;
  std::vector<std::uint8_t> response;
  std::string http_status;
  bool closed = false;
};

using StreamStatePtr = std::shared_ptr<StreamState>;
using StreamStateMap = std::map<std::int32_t, StreamStatePtr>;

StreamStatePtr find_stream_state(StreamStateMap& streams, std::int32_t stream_id) {
  const auto found = streams.find(stream_id);
  if (found == streams.end()) return nullptr;
  return found->second;
}

void mark_stream_close_requested(StreamState& state) {
  state.request_body.close_requested = true;
  state.closed = true;
}

void record_response_status(StreamState& state, std::string_view status) { state.http_status.assign(status); }

void append_response_body(StreamState& state, const std::uint8_t* data, std::size_t len) {
  state.response.insert(state.response.end(), data, data + len);
}

void mark_response_end_stream(StreamState& state) { state.closed = true; }

void erase_stream_state(StreamStateMap& streams, std::int32_t stream_id) { streams.erase(stream_id); }

void fail_bad_status(const StreamState& state) {
  if (state.http_status.empty() || state.http_status == "200") return;
  throw SdkError(error_with_message(ErrorKind::Transport, "gateway returned HTTP status " + state.http_status));
}

#ifndef CRABKA_CPP_HAS_NGHTTP2
PublishResult unavailable_publish() { throw SdkError(live_transport_unavailable_error()); }

MessageStream unavailable_subscribe() { throw SdkError(live_transport_unavailable_error()); }
#else
std::optional<std::string> http2_parse_failure_message(ssize_t parsed) {
  if (parsed >= 0) return std::nullopt;
  return crabka::detail::http2_parse_failure_message(
      static_cast<std::int64_t>(parsed), nghttp2_strerror(static_cast<int>(parsed)));
}

class UniqueFd {
public:
  explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}

  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this == &other) return *this;
    reset(other.release());
    return *this;
  }

  ~UniqueFd() { reset(); }

  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

  int release() noexcept {
    const auto fd = fd_;
    fd_ = -1;
    return fd;
  }

  void reset(int fd = -1) noexcept {
    if (fd_ >= 0) (void)::close(fd_);
    fd_ = fd;
  }

private:
  int fd_ = -1;
};

struct WakePipe {
  UniqueFd read_end;
  UniqueFd write_end;
};

struct Nghttp2SessionDeleter {
  void operator()(nghttp2_session* session) const noexcept {
    if (session != nullptr) nghttp2_session_del(session);
  }
};

using UniqueNghttp2Session = std::unique_ptr<nghttp2_session, Nghttp2SessionDeleter>;

class ScopedSigpipeSuppression {
public:
  ScopedSigpipeSuppression() noexcept {
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) && defined(SIGPIPE)
    struct sigaction ignore_action {};
    ignore_action.sa_handler = SIG_IGN;
    sigemptyset(&ignore_action.sa_mask);
    installed_ = sigaction(SIGPIPE, &ignore_action, &previous_action_) == 0;
#endif
  }

  ScopedSigpipeSuppression(const ScopedSigpipeSuppression&) = delete;
  ScopedSigpipeSuppression& operator=(const ScopedSigpipeSuppression&) = delete;

  ~ScopedSigpipeSuppression() {
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) && defined(SIGPIPE)
    if (installed_) (void)sigaction(SIGPIPE, &previous_action_, nullptr);
#endif
  }

private:
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) && defined(SIGPIPE)
  struct sigaction previous_action_ {};
  bool installed_ = false;
#endif
};

std::string errno_error_message(std::string_view action, int error_number) {
  return std::string(action) + ": " + std::strerror(error_number);
}

void check_file_control(int code, std::string_view action) {
  if (code >= 0) return;
  throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message(action, errno)));
}

void set_fd_flag(int fd, int get_command, int set_command, int flag, std::string_view action) {
  const auto existing = fcntl(fd, get_command);
  check_file_control(existing, action);
  check_file_control(fcntl(fd, set_command, existing | flag), action);
}

void prepare_wake_pipe_fd(const UniqueFd& fd) {
  set_fd_flag(fd.get(), F_GETFD, F_SETFD, FD_CLOEXEC, "configure wake pipe close-on-exec");
  set_fd_flag(fd.get(), F_GETFL, F_SETFL, O_NONBLOCK, "configure wake pipe nonblocking mode");
}

WakePipe make_wake_pipe() {
  int fds[2] = {-1, -1};
  if (pipe(fds) != 0) {
    throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message("create HTTP/2 cancellation pipe", errno)));
  }

  WakePipe pipe{.read_end = UniqueFd(fds[0]), .write_end = UniqueFd(fds[1])};
  prepare_wake_pipe_fd(pipe.read_end);
  prepare_wake_pipe_fd(pipe.write_end);
  return pipe;
}

void suppress_socket_sigpipe(int fd) {
#if !defined(MSG_NOSIGNAL) && defined(SO_NOSIGPIPE)
  const int enabled = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) != 0) {
    throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message("configure SIGPIPE-safe socket writes", errno)));
  }
#else
  (void)fd;
#endif
}

ssize_t send_without_sigpipe(int fd, const std::uint8_t* data, std::size_t length) {
  const ScopedSigpipeSuppression sigpipe_guard;
  return ::send(fd, data, length, sigpipe_safe_send_flags());
}

bool is_nonblocking_retry(int error_number) {
  if (error_number == EINTR) return true;
  if (error_number == EAGAIN) return true;
#ifdef EWOULDBLOCK
  if (error_number == EWOULDBLOCK) return true;
#endif
  return false;
}

class Http2Connection {
public:
  explicit Http2Connection(const Endpoint& endpoint)
      : endpoint_(endpoint), fd_(connect_socket(endpoint)), wake_pipe_(make_wake_pipe()) {
    nghttp2_session_callbacks* callbacks = nullptr;
    if (nghttp2_session_callbacks_new(&callbacks) != 0) throw SdkError(error_with_message(ErrorKind::Transport, "failed to allocate nghttp2 callbacks"));
    const std::unique_ptr<nghttp2_session_callbacks, decltype(&nghttp2_session_callbacks_del)> callback_guard(callbacks, nghttp2_session_callbacks_del);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame_recv);
    nghttp2_session* session = nullptr;
    if (nghttp2_session_client_new(&session, callbacks, this) != 0) throw SdkError(error_with_message(ErrorKind::Transport, "failed to create nghttp2 session"));
    session_.reset(session);
    const nghttp2_settings_entry settings[] = {{NGHTTP2_SETTINGS_ENABLE_PUSH, 0}};
    check_nghttp2(nghttp2_submit_settings(session_.get(), NGHTTP2_FLAG_NONE, settings, 1), "submit settings");
    flush_locked();
  }

  Http2Connection(const Http2Connection&) = delete;
  Http2Connection& operator=(const Http2Connection&) = delete;

  ~Http2Connection() = default;

  std::int32_t submit(LiveTransportRequestForTest request) {
    std::lock_guard guard(mutex_);
    auto state = std::make_shared<StreamState>();
    state->request_body = request_body_state(std::move(request.body), request.end_stream_after_body);

    std::vector<nghttp2_nv> headers;
    add_header(headers, ":method", "POST");
    add_header(headers, ":scheme", "http");
    add_header(headers, ":authority", endpoint_.authority);
    add_header(headers, ":path", request.path);
    for (const auto& header : request.headers) add_header(headers, header.name, header.value);

    nghttp2_data_provider provider{};
    provider.source.ptr = state.get();
    provider.read_callback = read_request;
    const auto stream_id = nghttp2_submit_request(session_.get(), nullptr, headers.data(), headers.size(), &provider, state.get());
    if (stream_id < 0) throw SdkError(error_with_message(ErrorKind::Transport, "failed to submit HTTP/2 request"));
    streams_.emplace(stream_id, std::move(state));
    flush_locked();
    return stream_id;
  }

  std::vector<std::uint8_t> read_message(std::int32_t stream_id, std::uint64_t timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock lock(mutex_);
    const auto state = stream_state_locked(stream_id);
    while (!state->closed) {
      throw_if_stream_cancelled(*state);
      if (auto message = take_message(state->response)) return *message;
      receive_until_locked(lock, deadline);
    }
    throw_if_stream_cancelled(*state);
    if (auto message = take_message(state->response)) return *message;
    fail_bad_status(*state);
    throw SdkError(error_with_message(ErrorKind::NotFound, "stream ended without a message"));
  }

  std::vector<std::uint8_t> read_unary(std::int32_t stream_id) {
    std::unique_lock lock(mutex_);
    const auto state = stream_state_locked(stream_id);
    while (!state->closed) receive_until_locked(lock, std::chrono::steady_clock::now() + std::chrono::seconds(10));
    fail_bad_status(*state);
    return state->response;
  }

  void close_stream(std::int32_t stream_id) {
    std::lock_guard guard(mutex_);
    const auto state = find_stream_state(streams_, stream_id);
    if (state == nullptr) return;
    mark_stream_close_requested(*state);
    wake_blocked_readers();
    (void)nghttp2_session_resume_data(session_.get(), stream_id);
    (void)nghttp2_submit_rst_stream(session_.get(), NGHTTP2_FLAG_NONE, stream_id, NGHTTP2_CANCEL);
    flush_locked();
  }

private:
  static UniqueFd connect_socket(const Endpoint& endpoint) {
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    addrinfo* result = nullptr;
    const int gai = getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &result);
    if (gai != 0) throw SdkError(error_with_message(ErrorKind::Transport, gai_strerror(gai)));
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> addresses(result, freeaddrinfo);
    for (addrinfo* address = addresses.get(); address != nullptr; address = address->ai_next) {
      UniqueFd fd(socket(address->ai_family, address->ai_socktype, address->ai_protocol));
      if (!fd.valid()) continue;
      suppress_socket_sigpipe(fd.get());
      if (connect(fd.get(), address->ai_addr, address->ai_addrlen) == 0) return fd;
    }
    throw SdkError(error_with_message(ErrorKind::Transport, "failed to connect to h2c endpoint"));
  }

  static void add_header(std::vector<nghttp2_nv>& headers, std::string_view name, std::string_view value) {
    nghttp2_nv header{};
    header.name = reinterpret_cast<std::uint8_t*>(const_cast<char*>(name.data()));
    header.value = reinterpret_cast<std::uint8_t*>(const_cast<char*>(value.data()));
    header.namelen = name.size();
    header.valuelen = value.size();
    header.flags = NGHTTP2_NV_FLAG_NONE;
    headers.push_back(header);
  }

  static ssize_t read_request(nghttp2_session*, std::int32_t, std::uint8_t* buffer, std::size_t length,
                              std::uint32_t* data_flags, nghttp2_data_source* source, void*) {
    auto* state = static_cast<StreamState*>(source->ptr);
    const auto read = read_request_body_chunk(state->request_body, buffer, length);
    if (read.deferred) return NGHTTP2_ERR_DEFERRED;
    if (read.end_stream) *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return static_cast<ssize_t>(read.copied);
  }

  static int on_header(nghttp2_session*, const nghttp2_frame* frame, const std::uint8_t* name, std::size_t namelen,
                       const std::uint8_t* value, std::size_t valuelen, std::uint8_t, void* user_data) {
    if (frame->hd.type != NGHTTP2_HEADERS) return 0;
    auto* connection = static_cast<Http2Connection*>(user_data);
    auto found = connection->streams_.find(frame->hd.stream_id);
    if (found == connection->streams_.end()) return 0;
    if (found->second->request_body.close_requested) return 0;
    const std::string_view header_name(reinterpret_cast<const char*>(name), namelen);
    if (header_name == ":status") {
      record_response_status(*found->second,
                             std::string_view(reinterpret_cast<const char*>(value), valuelen));
    }
    return 0;
  }

  static int on_data_chunk(nghttp2_session*, std::uint8_t, std::int32_t stream_id, const std::uint8_t* data,
                           std::size_t len, void* user_data) {
    auto* connection = static_cast<Http2Connection*>(user_data);
    auto found = connection->streams_.find(stream_id);
    if (found == connection->streams_.end()) return 0;
    if (found->second->request_body.close_requested) return 0;
    append_response_body(*found->second, data, len);
    return 0;
  }

  static int on_stream_close(nghttp2_session*, std::int32_t stream_id, std::uint32_t, void* user_data) {
    auto* connection = static_cast<Http2Connection*>(user_data);
    auto found = connection->streams_.find(stream_id);
    if (found == connection->streams_.end()) return 0;
    const auto close_requested = found->second->request_body.close_requested;
    mark_response_end_stream(*found->second);
    if (close_requested) erase_stream_state(connection->streams_, stream_id);
    return 0;
  }

  static int on_frame_recv(nghttp2_session*, const nghttp2_frame* frame, void* user_data) {
    if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) return 0;
    auto* connection = static_cast<Http2Connection*>(user_data);
    auto found = connection->streams_.find(frame->hd.stream_id);
    if (found == connection->streams_.end()) return 0;
    const auto close_requested = found->second->request_body.close_requested;
    mark_response_end_stream(*found->second);
    if (close_requested) erase_stream_state(connection->streams_, frame->hd.stream_id);
    return 0;
  }

  static void check_nghttp2(int code, std::string_view action) {
    if (code == 0) return;
    throw SdkError(error_with_message(ErrorKind::Transport, std::string(action) + " failed: " + nghttp2_strerror(code)));
  }

  static void check_nghttp2_parse_result(ssize_t parsed) {
    const auto failure = http2_parse_failure_message(parsed);
    if (!failure.has_value()) return;
    throw SdkError(error_with_message(ErrorKind::Transport, *failure));
  }

  static std::optional<std::vector<std::uint8_t>> take_message(std::vector<std::uint8_t>& bytes) {
    const auto decoded = envelope::decode_one(bytes);
    if (std::holds_alternative<envelope::NeedMore>(decoded)) return std::nullopt;
    if (auto end = std::get_if<envelope::EndStream>(&decoded); end != nullptr) {
      if (end->code.has_value()) throw SdkError(error_with_message(ErrorKind::Transport, *end->message));
      bytes.clear();
      return std::nullopt;
    }
    auto message = std::get<envelope::Message>(decoded);
    const auto consumed = message.payload.size() + 5;
    bytes.erase(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(consumed));
    return message.payload;
  }

  static void throw_if_stream_cancelled(const StreamState& state) {
    if (!state.request_body.close_requested) return;
    throw SdkError(error_with_message(ErrorKind::NotFound, "subscription is closed"));
  }

  void flush_locked() {
    const std::uint8_t* data = nullptr;
    while (const auto length = nghttp2_session_mem_send(session_.get(), &data)) {
      if (length < 0) check_nghttp2(static_cast<int>(length), "serialize HTTP/2 data");
      std::size_t cursor = 0;
      while (cursor < static_cast<std::size_t>(length)) {
        const auto sent = send_without_sigpipe(fd_.get(), data + cursor, static_cast<std::size_t>(length) - cursor);
        if (sent < 0) {
          const auto error_number = errno;
          if (error_number == EINTR) continue;
          throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message("failed to write HTTP/2 data", error_number)));
        }
        if (sent == 0) throw SdkError(error_with_message(ErrorKind::Transport, "HTTP/2 socket accepted zero write bytes"));
        cursor += static_cast<std::size_t>(sent);
      }
    }
  }

  void receive_until_locked(std::unique_lock<std::mutex>& lock, std::chrono::steady_clock::time_point deadline) {
    flush_locked();
    for (;;) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) throw SdkError(error_with_message(ErrorKind::Transport, "HTTP/2 response timed out"));
      const auto timeout = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
      pollfd descriptors[2]{};
      descriptors[0].fd = fd_.get();
      descriptors[0].events = POLLIN;
      descriptors[1].fd = wake_pipe_.read_end.get();
      descriptors[1].events = POLLIN;

      lock.unlock();
      const auto ready = poll(descriptors, 2, timeout);
      lock.lock();

      if (ready == 0) throw SdkError(error_with_message(ErrorKind::Transport, "HTTP/2 response timed out"));
      if (ready < 0) {
        if (errno == EINTR) continue;
        throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message("poll failed while reading HTTP/2 response", errno)));
      }
      if ((descriptors[1].revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL)) != 0) {
        drain_wake_pipe();
        return;
      }
      if ((descriptors[0].revents & POLLIN) != 0) {
        receive_available_bytes_locked();
        return;
      }
      if ((descriptors[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        throw SdkError(error_with_message(ErrorKind::Transport, "HTTP/2 socket closed"));
      }
    }
  }

  void receive_available_bytes_locked() {
    std::uint8_t buffer[16384];
    const auto received = recv(fd_.get(), buffer, sizeof(buffer), nonblocking_receive_flags());
    if (received < 0) {
      const auto error_number = errno;
      if (is_nonblocking_retry(error_number)) return;
      throw SdkError(error_with_message(ErrorKind::Transport, errno_error_message("failed to read HTTP/2 data", error_number)));
    }
    if (received == 0) throw SdkError(error_with_message(ErrorKind::Transport, "HTTP/2 socket closed"));
    const auto parsed = nghttp2_session_mem_recv(session_.get(), buffer, static_cast<std::size_t>(received));
    check_nghttp2_parse_result(parsed);
  }

  void wake_blocked_readers() noexcept {
    const std::uint8_t byte = 1;
    const auto written = ::write(wake_pipe_.write_end.get(), &byte, sizeof(byte));
    if (written >= 0) return;
    if (is_nonblocking_retry(errno)) return;
  }

  void drain_wake_pipe() noexcept {
    std::uint8_t buffer[64];
    for (;;) {
      const auto read = ::read(wake_pipe_.read_end.get(), buffer, sizeof(buffer));
      if (read > 0) continue;
      if (read == 0) return;
      if (is_nonblocking_retry(errno)) return;
      return;
    }
  }

  StreamStatePtr stream_state_locked(std::int32_t stream_id) {
    auto state = find_stream_state(streams_, stream_id);
    if (state == nullptr) throw SdkError(error_with_message(ErrorKind::Transport, "unknown HTTP/2 stream"));
    return state;
  }

  Endpoint endpoint_;
  UniqueFd fd_;
  WakePipe wake_pipe_;
  UniqueNghttp2Session session_;
  std::mutex mutex_;
  StreamStateMap streams_;
};

class Nghttp2Subscription final : public LiveSubscription {
public:
  Nghttp2Subscription(Endpoint endpoint, LiveTransportRequestForTest request)
      : connection_(std::make_unique<Http2Connection>(endpoint)) {
    stream_id_ = connection_->submit(std::move(request));
  }

  ~Nghttp2Subscription() override {
    try {
      close();
    } catch (...) {
    }
  }

  [[nodiscard]] Inbound next(std::uint64_t timeout_ms) override {
    {
      std::lock_guard guard(mutex_);
      if (closed_) throw SdkError(error_with_message(ErrorKind::NotFound, "subscription is closed"));
    }
    return decode_inbound(connection_->read_message(stream_id_, timeout_ms));
  }

  void close() override {
    {
      std::lock_guard guard(mutex_);
      if (closed_) return;
      closed_ = true;
    }
    connection_->close_stream(stream_id_);
  }

private:
  std::unique_ptr<Http2Connection> connection_;
  std::int32_t stream_id_ = 0;
  std::mutex mutex_;
  bool closed_ = false;
};
#endif
} // namespace

Error live_transport_unavailable_error() {
  return error_with_message(ErrorKind::Transport, "live nghttp2 transport is not available in this build");
}

PublishResult live_transport_publish(const std::string& endpoint, const std::optional<std::string>& bearer,
                                     const Record& record) {
#ifndef CRABKA_CPP_HAS_NGHTTP2
  (void)endpoint;
  (void)bearer;
  (void)record;
  return unavailable_publish();
#else
  const auto parsed = parse_endpoint(endpoint);
  Http2Connection connection(parsed);
  const auto stream_id = connection.submit(make_send_http_request(record, bearer));
  return decode_send_response(connection.read_unary(stream_id));
#endif
}

MessageStream live_transport_subscribe(const std::string& endpoint, const std::optional<std::string>& bearer,
                                       const std::vector<std::string>& topics, const std::string& group,
                                       const std::optional<Filter>& filter) {
#ifndef CRABKA_CPP_HAS_NGHTTP2
  (void)endpoint;
  (void)bearer;
  (void)topics;
  (void)group;
  (void)filter;
  return unavailable_subscribe();
#else
  return MessageStream(std::make_shared<Nghttp2Subscription>(
      parse_endpoint(endpoint), make_subscribe_http_request(topics, group, filter, bearer)));
#endif
}

std::vector<std::uint8_t> live_transport_send_request_bytes_for_test(const Record& record) {
  return encode_send_request(record);
}

LiveTransportRequestForTest live_transport_send_http_request_for_test(
    const Record& record, const std::optional<std::string>& bearer) {
  return make_send_http_request(record, bearer);
}

std::vector<std::uint8_t> live_transport_subscribe_start_bytes_for_test(const std::vector<std::string>& topics,
                                                                        const std::string& group,
                                                                        const std::optional<Filter>& filter) {
  return encode_subscribe_start(topics, group, filter);
}

LiveTransportRequestForTest live_transport_subscribe_http_request_for_test(
    const std::vector<std::string>& topics, const std::string& group,
    const std::optional<Filter>& filter, const std::optional<std::string>& bearer) {
  return make_subscribe_http_request(topics, group, filter, bearer);
}

LiveTransportRequestBodyReadPlanForTest live_transport_request_body_read_plan_for_test(
    const LiveTransportRequestForTest& request) {
  auto state = request_body_state(request.body, request.end_stream_after_body);
  std::vector<std::uint8_t> buffer(std::max<std::size_t>(request.body.size(), 1));
  const auto body_read = read_request_body_chunk(state, buffer.data(), buffer.size());
  const auto after_body_read = read_request_body_chunk(state, buffer.data(), buffer.size());
  state.close_requested = true;
  const auto close_read = read_request_body_chunk(state, buffer.data(), buffer.size());
  auto close_before_drained_state = request_body_state(request.body, request.end_stream_after_body);
  close_before_drained_state.close_requested = true;
  std::vector<std::uint8_t> single_byte_buffer(1);
  const auto close_before_drained_read = read_request_body_chunk(
      close_before_drained_state, single_byte_buffer.data(), single_byte_buffer.size());
  const auto close_after_drained_read = read_request_body_chunk(
      close_before_drained_state, buffer.data(), buffer.size());
  return LiveTransportRequestBodyReadPlanForTest{.final_body_read_ends_stream = body_read.end_stream,
                                                 .read_after_body_defers = after_body_read.deferred,
                                                 .close_after_body_ends_stream = close_read.end_stream,
                                                 .close_before_body_drained_copies_body = close_before_drained_read.copied == 1,
                                                 .close_before_body_drained_ends_stream = close_before_drained_read.end_stream,
                                                 .close_before_body_drained_ends_stream_after_body = close_after_drained_read.end_stream};
}

LiveTransportStreamClosePlanForTest live_transport_stream_close_plan_for_test() {
  StreamStateMap streams;
  auto state = std::make_shared<StreamState>();
  state->request_body = request_body_state({1, 2, 3}, false);
  streams.emplace(7, state);

  const auto reader_state = find_stream_state(streams, 7);
  mark_stream_close_requested(*state);
  const auto close_marks_request_body_closed =
      reader_state->request_body.close_requested && reader_state->closed;
  const auto close_keeps_stream_owner = find_stream_state(streams, 7) != nullptr;
  erase_stream_state(streams, 7);
  const auto protocol_close_removes_stream_owner = find_stream_state(streams, 7) == nullptr;
  const auto reader_state_survives_protocol_close =
      reader_state->request_body.close_requested && reader_state->closed;

  return LiveTransportStreamClosePlanForTest{
      .close_marks_request_body_closed = close_marks_request_body_closed,
      .close_keeps_stream_owner = close_keeps_stream_owner,
      .protocol_close_removes_stream_owner = protocol_close_removes_stream_owner,
      .reader_state_survives_protocol_close = reader_state_survives_protocol_close};
}

LiveTransportSendSafetyForTest live_transport_send_safety_for_test() {
  return LiveTransportSendSafetyForTest{.uses_msg_nosignal = sigpipe_safe_send_flags() != 0,
                                       .suppresses_sigpipe = transport_writes_suppress_sigpipe()};
}

LiveTransportResponseLifecycleForTest live_transport_response_lifecycle_for_test() {
  StreamState success;
  record_response_status(success, "200");
  const std::uint8_t body[] = {'o', 'k'};
  append_response_body(success, body, sizeof(body));
  mark_response_end_stream(success);

  bool success_status_allows_response = false;
  try {
    fail_bad_status(success);
    success_status_allows_response = true;
  } catch (const SdkError&) {
  }

  StreamState bad_status;
  record_response_status(bad_status, "500");
  mark_response_end_stream(bad_status);
  bool bad_status_fails_after_close = false;
  try {
    fail_bad_status(bad_status);
  } catch (const SdkError&) {
    bad_status_fails_after_close = true;
  }

  return LiveTransportResponseLifecycleForTest{
      .success_status_allows_response = success_status_allows_response,
      .bad_status_fails_after_close = bad_status_fails_after_close,
      .data_before_close_is_preserved = success.response == std::vector<std::uint8_t>({'o', 'k'}),
      .end_stream_closes_stream = success.closed && bad_status.closed};
}

} // namespace crabka

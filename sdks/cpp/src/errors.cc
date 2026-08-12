#include "crabka/errors.hpp"

#include <utility>

namespace crabka {

std::string to_string(ErrorKind kind) {
  switch (kind) {
  case ErrorKind::Transport:
    return "transport";
  case ErrorKind::Unauthenticated:
    return "unauthenticated";
  case ErrorKind::InvalidArgument:
    return "invalid_argument";
  case ErrorKind::NotFound:
    return "not_found";
  case ErrorKind::ServerError:
    return "server_error";
  case ErrorKind::Unimplemented:
    return "unimplemented";
  }
  return "server_error";
}

Error error_with_message(ErrorKind kind, std::string message) {
  return Error{.kind = kind, .module = "", .gated_on = "", .message = std::move(message)};
}

Error unimplemented_module(std::string module, std::string gated_on) {
  return Error{.kind = ErrorKind::Unimplemented,
               .module = std::move(module),
               .gated_on = std::move(gated_on),
               .message = ""};
}

SdkError::SdkError(Error error) : std::runtime_error(error.message), error_(std::move(error)) {}

const Error& SdkError::error() const noexcept { return error_; }

} // namespace crabka

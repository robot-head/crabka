#pragma once

#include <stdexcept>
#include <string>

namespace crabka {

enum class ErrorKind {
  Transport,
  Unauthenticated,
  InvalidArgument,
  NotFound,
  ServerError,
  Unimplemented,
};

struct Error {
  ErrorKind kind;
  std::string module;
  std::string gated_on;
  std::string message;
};

[[nodiscard]] std::string to_string(ErrorKind kind);
[[nodiscard]] Error error_with_message(ErrorKind kind, std::string message);
[[nodiscard]] Error unimplemented_module(std::string module, std::string gated_on);

class SdkError final : public std::runtime_error {
public:
  explicit SdkError(Error error);
  [[nodiscard]] const Error& error() const noexcept;

private:
  Error error_;
};

} // namespace crabka

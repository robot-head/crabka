#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace crabka::detail {

[[nodiscard]] inline std::optional<std::string> http2_parse_failure_message(
    std::int64_t parsed, std::string_view error_message) {
  if (parsed >= 0) return std::nullopt;
  return std::string("parse HTTP/2 data failed: ") + std::string(error_message);
}

} // namespace crabka::detail

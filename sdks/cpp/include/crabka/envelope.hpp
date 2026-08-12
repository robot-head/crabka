#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace crabka::envelope {

struct Message {
  std::uint8_t flags;
  std::vector<std::uint8_t> payload;
};

struct EndStream {
  std::optional<std::string> code;
  std::optional<std::string> message;
};

struct NeedMore {
  std::size_t bytes;
};

using DecodeResult = std::variant<Message, EndStream, NeedMore>;

[[nodiscard]] std::vector<std::uint8_t> encode(std::uint8_t flags,
                                               const std::vector<std::uint8_t>& payload);
[[nodiscard]] DecodeResult decode_one(const std::vector<std::uint8_t>& bytes);

} // namespace crabka::envelope

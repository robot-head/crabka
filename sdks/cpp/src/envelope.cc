#include "crabka/envelope.hpp"
#include "crabka/json.hpp"

#include <string>

namespace crabka::envelope {
namespace {
constexpr std::uint8_t kEndStreamFlag = 0x02;
}

std::vector<std::uint8_t> encode(std::uint8_t flags, const std::vector<std::uint8_t>& payload) {
  std::vector<std::uint8_t> out;
  out.reserve(payload.size() + 5);
  out.push_back(flags);
  const auto size = static_cast<std::uint32_t>(payload.size());
  out.push_back(static_cast<std::uint8_t>((size >> 24U) & 255U));
  out.push_back(static_cast<std::uint8_t>((size >> 16U) & 255U));
  out.push_back(static_cast<std::uint8_t>((size >> 8U) & 255U));
  out.push_back(static_cast<std::uint8_t>(size & 255U));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

DecodeResult decode_one(const std::vector<std::uint8_t>& bytes) {
  if (bytes.size() < 5) return NeedMore{5 - bytes.size()};
  const std::uint8_t flags = bytes[0];
  const std::uint32_t size = (static_cast<std::uint32_t>(bytes[1]) << 24U) |
                             (static_cast<std::uint32_t>(bytes[2]) << 16U) |
                             (static_cast<std::uint32_t>(bytes[3]) << 8U) |
                             static_cast<std::uint32_t>(bytes[4]);
  if (bytes.size() < static_cast<std::size_t>(size) + 5U) {
    return NeedMore{static_cast<std::size_t>(size) + 5U - bytes.size()};
  }
  std::vector<std::uint8_t> payload(bytes.begin() + 5, bytes.begin() + 5 + size);
  if ((flags & kEndStreamFlag) == 0U) return Message{flags, std::move(payload)};
  EndStream end;
  if (payload.empty()) return end;
  const std::string text(payload.begin(), payload.end());
  const auto object = json::as_object(json::parse(text));
  if (auto error = object.find("error"); error != object.end() && !std::holds_alternative<std::nullptr_t>(error->second.data)) {
    const auto& error_object = json::as_object(error->second);
    if (auto code = error_object.find("code"); code != error_object.end()) end.code = json::as_string(code->second);
    if (auto message = error_object.find("message"); message != error_object.end()) end.message = json::as_string(message->second);
  }
  return end;
}

} // namespace crabka::envelope

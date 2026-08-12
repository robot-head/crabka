#include "crabka/base64.hpp"
#include "crabka/errors.hpp"

#include <array>

namespace crabka {
namespace {
constexpr char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

int decode_char(char c) {
  if (c >= 'A' && c <= 'Z') return c - 'A';
  if (c >= 'a' && c <= 'z') return c - 'a' + 26;
  if (c >= '0' && c <= '9') return c - '0' + 52;
  if (c == '+') return 62;
  if (c == '/') return 63;
  if (c == '=') return -2;
  return -1;
}
} // namespace

std::string base64_encode(const std::vector<std::uint8_t>& bytes) {
  std::string out;
  out.reserve(((bytes.size() + 2) / 3) * 4);
  for (std::size_t i = 0; i < bytes.size(); i += 3) {
    const std::uint32_t a = bytes[i];
    const std::uint32_t b = i + 1 < bytes.size() ? bytes[i + 1] : 0;
    const std::uint32_t c = i + 2 < bytes.size() ? bytes[i + 2] : 0;
    const std::uint32_t n = (a << 16U) | (b << 8U) | c;
    out.push_back(alphabet[(n >> 18U) & 63U]);
    out.push_back(alphabet[(n >> 12U) & 63U]);
    out.push_back(i + 1 < bytes.size() ? alphabet[(n >> 6U) & 63U] : '=');
    out.push_back(i + 2 < bytes.size() ? alphabet[n & 63U] : '=');
  }
  return out;
}

std::vector<std::uint8_t> base64_decode(const std::string& text) {
  if (text.size() % 4 != 0) {
    throw SdkError(error_with_message(ErrorKind::InvalidArgument, "invalid base64 length"));
  }
  std::vector<std::uint8_t> out;
  out.reserve((text.size() / 4) * 3);
  for (std::size_t i = 0; i < text.size(); i += 4) {
    std::array<int, 4> q{decode_char(text[i]), decode_char(text[i + 1]), decode_char(text[i + 2]),
                         decode_char(text[i + 3])};
    if (q[0] < 0 || q[1] < 0 || q[2] == -1 || q[3] == -1) {
      throw SdkError(error_with_message(ErrorKind::InvalidArgument, "invalid base64 character"));
    }
    const std::uint32_t n = (static_cast<std::uint32_t>(q[0]) << 18U) |
                            (static_cast<std::uint32_t>(q[1]) << 12U) |
                            (q[2] < 0 ? 0U : static_cast<std::uint32_t>(q[2]) << 6U) |
                            (q[3] < 0 ? 0U : static_cast<std::uint32_t>(q[3]));
    out.push_back(static_cast<std::uint8_t>((n >> 16U) & 255U));
    if (q[2] != -2) out.push_back(static_cast<std::uint8_t>((n >> 8U) & 255U));
    if (q[3] != -2) out.push_back(static_cast<std::uint8_t>(n & 255U));
  }
  return out;
}

} // namespace crabka

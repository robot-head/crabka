#include "crabka/json.hpp"
#include "crabka/errors.hpp"

#include <cctype>
#include <charconv>
#include <sstream>

namespace crabka::json {
namespace {
class Parser {
public:
  explicit Parser(const std::string& text) : text_(text) {}
  Value parse_value() {
    skip_ws();
    if (pos_ >= text_.size()) fail("unexpected end of json");
    if (text_[pos_] == '{') return parse_object();
    if (text_[pos_] == '[') return parse_array();
    if (text_[pos_] == '"') return Value{parse_string()};
    if (text_.compare(pos_, 4, "null") == 0) { pos_ += 4; return Value{nullptr}; }
    if (text_.compare(pos_, 4, "true") == 0) { pos_ += 4; return Value{true}; }
    if (text_.compare(pos_, 5, "false") == 0) { pos_ += 5; return Value{false}; }
    return parse_number();
  }
  void finish() { skip_ws(); if (pos_ != text_.size()) fail("trailing json input"); }

private:
  const std::string& text_;
  std::size_t pos_ = 0;
  [[noreturn]] void fail(const std::string& message) const { throw SdkError(error_with_message(ErrorKind::InvalidArgument, message)); }
  void skip_ws() { while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_]))) ++pos_; }
  bool take(char c) { skip_ws(); if (pos_ < text_.size() && text_[pos_] == c) { ++pos_; return true; } return false; }
  Value parse_object() {
    Object object;
    ++pos_;
    if (take('}')) return Value{object};
    while (true) {
      skip_ws();
      if (pos_ >= text_.size() || text_[pos_] != '"') fail("object key must be a string");
      std::string key = parse_string();
      if (!take(':')) fail("object key missing colon");
      object.emplace(std::move(key), parse_value());
      if (take('}')) return Value{object};
      if (!take(',')) fail("object entry missing comma");
    }
  }
  Value parse_array() {
    Array array;
    ++pos_;
    if (take(']')) return Value{array};
    while (true) {
      array.push_back(parse_value());
      if (take(']')) return Value{array};
      if (!take(',')) fail("array entry missing comma");
    }
  }
  std::string parse_string() {
    std::string out;
    ++pos_;
    while (pos_ < text_.size()) {
      char c = text_[pos_++];
      if (c == '"') return out;
      if (c != '\\') { out.push_back(c); continue; }
      if (pos_ >= text_.size()) fail("unterminated escape");
      char e = text_[pos_++];
      if (e == '"' || e == '\\' || e == '/') out.push_back(e);
      else if (e == 'b') out.push_back('\b');
      else if (e == 'f') out.push_back('\f');
      else if (e == 'n') out.push_back('\n');
      else if (e == 'r') out.push_back('\r');
      else if (e == 't') out.push_back('\t');
      else fail("unsupported json escape");
    }
    fail("unterminated string");
  }
  Value parse_number() {
    const std::size_t begin = pos_;
    if (text_[pos_] == '-') ++pos_;
    while (pos_ < text_.size() && std::isdigit(static_cast<unsigned char>(text_[pos_]))) ++pos_;
    if (pos_ < text_.size() && text_[pos_] == '.') {
      ++pos_;
      while (pos_ < text_.size() && std::isdigit(static_cast<unsigned char>(text_[pos_]))) ++pos_;
    }
    double number = 0;
    auto view = text_.substr(begin, pos_ - begin);
    auto result = std::from_chars(view.data(), view.data() + view.size(), number);
    if (result.ec != std::errc()) fail("invalid number");
    return Value{number};
  }
};

void append_escaped(std::ostringstream& out, const std::string& text) {
  out << '"';
  for (char c : text) {
    if (c == '"' || c == '\\') out << '\\' << c;
    else if (c == '\n') out << "\\n";
    else if (c == '\r') out << "\\r";
    else if (c == '\t') out << "\\t";
    else out << c;
  }
  out << '"';
}

void append_value(std::ostringstream& out, const Value& value) {
  if (std::holds_alternative<std::nullptr_t>(value.data)) { out << "null"; return; }
  if (auto boolean = std::get_if<bool>(&value.data)) { out << (*boolean ? "true" : "false"); return; }
  if (auto number = std::get_if<double>(&value.data)) { out << *number; return; }
  if (auto string = std::get_if<std::string>(&value.data)) { append_escaped(out, *string); return; }
  if (auto array = std::get_if<Array>(&value.data)) {
    out << '[';
    for (std::size_t i = 0; i < array->size(); ++i) { if (i != 0) out << ','; append_value(out, (*array)[i]); }
    out << ']';
    return;
  }
  const auto& object = std::get<Object>(value.data);
  out << '{';
  bool first = true;
  for (const auto& [key, child] : object) {
    if (!first) out << ',';
    first = false;
    append_escaped(out, key);
    out << ':';
    append_value(out, child);
  }
  out << '}';
}
} // namespace

Value parse(const std::string& text) { Parser parser(text); Value value = parser.parse_value(); parser.finish(); return value; }
std::string stringify(const Value& value) { std::ostringstream out; append_value(out, value); return out.str(); }
const Object& as_object(const Value& value) { if (auto object = std::get_if<Object>(&value.data)) return *object; throw SdkError(error_with_message(ErrorKind::InvalidArgument, "expected object")); }
const Array& as_array(const Value& value) { if (auto array = std::get_if<Array>(&value.data)) return *array; throw SdkError(error_with_message(ErrorKind::InvalidArgument, "expected array")); }
const std::string& as_string(const Value& value) { if (auto string = std::get_if<std::string>(&value.data)) return *string; throw SdkError(error_with_message(ErrorKind::InvalidArgument, "expected string")); }
std::string get_string(const Object& object, const std::string& key, const std::string& fallback) { auto found = object.find(key); return found == object.end() || std::holds_alternative<std::nullptr_t>(found->second.data) ? fallback : as_string(found->second); }
bool has_non_null(const Object& object, const std::string& key) { auto found = object.find(key); return found != object.end() && !std::holds_alternative<std::nullptr_t>(found->second.data); }

} // namespace crabka::json

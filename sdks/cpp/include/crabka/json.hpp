#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <variant>
#include <vector>

namespace crabka::json {

struct Value;
using Array = std::vector<Value>;
using Object = std::map<std::string, Value>;

struct Value {
  std::variant<std::nullptr_t, bool, double, std::string, Array, Object> data;
};

[[nodiscard]] Value parse(const std::string& text);
[[nodiscard]] std::string stringify(const Value& value);
[[nodiscard]] const Object& as_object(const Value& value);
[[nodiscard]] const Array& as_array(const Value& value);
[[nodiscard]] const std::string& as_string(const Value& value);
[[nodiscard]] std::string get_string(const Object& object, const std::string& key,
                                     const std::string& fallback = "");
[[nodiscard]] bool has_non_null(const Object& object, const std::string& key);

} // namespace crabka::json

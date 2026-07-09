cmake_minimum_required(VERSION 3.22)
project(crabka_sdk_cpp VERSION 0.1.0 LANGUAGES CXX)

option(CRABKA_CPP_SANITIZERS "Enable ASan/UBSan" ON)
option(CRABKA_CPP_TSAN "Enable TSan instead of ASan/UBSan" OFF)
option(CRABKA_CPP_REQUIRE_EXTERNAL_DEPS "Fail configure when libnghttp2 or protobuf are missing" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CRABKA_CPP_TSAN AND CRABKA_CPP_SANITIZERS)
  message(FATAL_ERROR "CRABKA_CPP_TSAN and CRABKA_CPP_SANITIZERS are mutually exclusive")
endif()

if(CRABKA_CPP_TSAN AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(CRABKA_CPP_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(NGHTTP2 IMPORTED_TARGET libnghttp2)
find_package(Protobuf QUIET)

if(CRABKA_CPP_REQUIRE_EXTERNAL_DEPS AND (NOT NGHTTP2_FOUND OR NOT Protobuf_FOUND))
  message(FATAL_ERROR "full C++ SDK gate requires libnghttp2 and protobuf development packages")
endif()

add_library(crabka_cpp
  src/base64.cc
  src/client.cc
  src/envelope.cc
  src/errors.cc
  src/json.cc
  src/stubs.cc
  src/transport_nghttp2.cc
)
target_include_directories(crabka_cpp PUBLIC include)
if(NGHTTP2_FOUND)
  target_link_libraries(crabka_cpp PUBLIC PkgConfig::NGHTTP2)
  target_compile_definitions(crabka_cpp PRIVATE CRABKA_CPP_HAS_NGHTTP2)
else()
  message(WARNING "libnghttp2 not found; live transport will return Transport errors")
endif()
if(NOT Protobuf_FOUND)
  message(WARNING "protobuf not found; generated gateway bindings are not part of this local gate")
endif()
target_compile_options(crabka_cpp PRIVATE -Wall -Wextra -Wpedantic)

add_executable(crabka_cpp_adapter adapter/main.cc)
target_link_libraries(crabka_cpp_adapter PRIVATE crabka_cpp)

enable_testing()
add_executable(crabka_cpp_codec_test test/codec_test.cc)
target_link_libraries(crabka_cpp_codec_test PRIVATE crabka_cpp)
target_compile_definitions(crabka_cpp_codec_test PRIVATE CRABKA_CPP_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/test/vectors")
add_test(NAME crabka_cpp_codec_test COMMAND crabka_cpp_codec_test)
add_executable(crabka_cpp_filter_test test/filter_test.cc)
target_link_libraries(crabka_cpp_filter_test PRIVATE crabka_cpp)
add_test(NAME crabka_cpp_filter_test COMMAND crabka_cpp_filter_test)
add_executable(crabka_cpp_transport_test test/transport_test.cc)
target_link_libraries(crabka_cpp_transport_test PRIVATE crabka_cpp)
target_include_directories(crabka_cpp_transport_test PRIVATE src)
add_test(NAME crabka_cpp_transport_test COMMAND crabka_cpp_transport_test)
set_tests_properties(crabka_cpp_transport_test PROPERTIES LABELS "transport;tsan")

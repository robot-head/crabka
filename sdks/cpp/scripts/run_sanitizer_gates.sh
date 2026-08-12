#!/usr/bin/env bash
set -euo pipefail

sdk_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cmake -S "${sdk_dir}" -B "${sdk_dir}/build-asan" -DCRABKA_CPP_SANITIZERS=ON -DCRABKA_CPP_TSAN=OFF -DCRABKA_CPP_REQUIRE_EXTERNAL_DEPS=OFF
cmake --build "${sdk_dir}/build-asan"
ctest --test-dir "${sdk_dir}/build-asan" --output-on-failure

cmake -S "${sdk_dir}" -B "${sdk_dir}/build-tsan" -DCRABKA_CPP_SANITIZERS=OFF -DCRABKA_CPP_TSAN=ON -DCRABKA_CPP_REQUIRE_EXTERNAL_DEPS=OFF
cmake --build "${sdk_dir}/build-tsan" --target crabka_cpp_transport_test
ctest --test-dir "${sdk_dir}/build-tsan" -L tsan --output-on-failure

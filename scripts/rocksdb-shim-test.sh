#!/usr/bin/env bash

# Tests the RocksDB shim in C, through the library the npm package ships, under a leak checker: leaks on
# macOS and LeakSanitizer on Linux. It runs rocksdb-shim-test.c three times: once with the RocksDB library
# named by path, then as a Native program finds it, beside the executable and through XKAFKA_ROCKSDB_LIBRARY.
#
# usage: rocksdb-shim-test.sh shim-library rocksdb-library version identity
#
# `sbt clientJS/rocksdbShimTest` builds both libraries and passes them here.

set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 shim-library rocksdb-library version identity" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
shim="$1"
rocksdb="$2"
version="$3"
identity="$4"
output="$project_dir/target/rocksdb-shim-test"
harness="$output/rocksdb-shim-test"
platform="$(uname -s)"

case "$platform" in
  Darwin) sanitizer="" ;;
  Linux)  sanitizer="-fsanitize=leak" ;;
  *)      echo "no leak checker configured for $platform" >&2; exit 2 ;;
esac

rm -rf "$output"
mkdir -p "$output"
# shellcheck disable=SC2086
cc -std=gnu11 -g -Wall -Wextra -Werror $sanitizer -o "$harness" "$script_dir/rocksdb-shim-test.c" "$shim" \
  -Wl,-rpath,"$(dirname "$shim")" -lpthread

# leaks answers for the leaks it found and not for how the program exited, so on macOS the harness's
# report of its own checks is what tells whether they passed.
run() {
  local directory log
  directory="$(mktemp -d "$output/store.XXXXXX")"
  case "$platform" in
    Darwin)
      log="$directory.log"
      leaks --quiet --atExit -- "$harness" "$@" "$directory" 2>&1 | tee "$log"
      grep -q "every check passed" "$log"
      ;;
    Linux) "$harness" "$@" "$directory" ;;
  esac
}

echo "Testing every call with the library named by path"
run "$rocksdb" "$version" "$identity"

echo "Testing the library found beside the executable"
cp "$rocksdb" "$output/"
(unset XKAFKA_ROCKSDB_LIBRARY; run --search "$version")
rm "$output/$(basename "$rocksdb")"

echo "Testing the library named by XKAFKA_ROCKSDB_LIBRARY"
XKAFKA_ROCKSDB_LIBRARY="$rocksdb" run --search "$version"

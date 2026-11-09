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

# GCC's LeakSanitizer is a shared library, which on x86_64 recurses without end through its own thread-local cache
# once loading RocksDB grows the thread-local tables; clang links its LeakSanitizer into the program instead.
case "$platform" in
  Darwin) compiler=cc;    sanitizer="" ;;
  Linux)  compiler=clang; sanitizer="-fsanitize=leak" ;;
  *)      echo "no leak checker configured for $platform" >&2; exit 2 ;;
esac
export LSAN_OPTIONS="suppressions=$script_dir/rocksdb-shim-test.lsan:print_suppressions=1${LSAN_OPTIONS:+:$LSAN_OPTIONS}"

rm -rf "$output"
mkdir -p "$output"
# shellcheck disable=SC2086
"$compiler" -std=gnu11 -g -Wall -Wextra -Werror $sanitizer -o "$harness" "$script_dir/rocksdb-shim-test.c" "$shim" \
  -Wl,-rpath,"$(dirname "$shim")" -lpthread

# A run takes seconds; one still going after this many is stuck, and its threads' stacks show where.
limit=300

# Prints the stack of every thread of the process given and of its children: leaks and the harness it started.
stacks() {
  local pid task
  for pid in "$1" $(pgrep -P "$1" || true); do
    echo "=== process $pid: $(ps -o command= -p "$pid" || true)"
    case "$platform" in
      Darwin) sample "$pid" 5 -file /dev/stdout || true ;;
      Linux)
        # A leak checker that stopped the threads holds them, so the kernel's view is read as well as the debugger's.
        for task in /proc/"$pid"/task/*; do
          echo "--- thread ${task##*/} $(cat "$task/comm" 2>/dev/null) state $(awk '{print $3}' "$task/stat" 2>/dev/null) in $(cat "$task/wchan" 2>/dev/null)"
          sudo cat "$task/stack" 2>/dev/null || true
        done
        timeout 60 sudo gdb -p "$pid" -batch -ex "thread apply all bt" 2>&1 || true
        ;;
    esac
  done
}

# leaks answers for the leaks it found and not for how the program exited, so on macOS the harness's
# report of its own checks is what tells whether they passed.
run() {
  local directory log pid waited=0
  directory="$(mktemp -d "$output/store.XXXXXX")"
  log="$directory.log"
  case "$platform" in
    Darwin) leaks --quiet --atExit -- "$harness" "$@" "$directory" > "$log" 2>&1 & pid=$! ;;
    Linux)  "$harness" "$@" "$directory" > "$log" 2>&1 & pid=$! ;;
  esac
  while kill -0 "$pid" 2>/dev/null; do
    if (( waited >= limit )); then
      cat "$log"
      echo "the harness was still running after $limit seconds; the stacks of its threads follow" >&2
      stacks "$pid"
      pkill -9 -P "$pid" || true
      kill -9 "$pid" || true
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$pid" || { cat "$log"; return 1; }
  cat "$log"
  grep -q "every check passed" "$log"
}

echo "Testing every call with the library named by path"
run "$rocksdb" "$version" "$identity"

echo "Testing the library found beside the executable"
cp "$rocksdb" "$output/"
(unset XKAFKA_ROCKSDB_LIBRARY; run --search "$version")
rm "$output/$(basename "$rocksdb")"

echo "Testing the library named by XKAFKA_ROCKSDB_LIBRARY"
XKAFKA_ROCKSDB_LIBRARY="$rocksdb" run --search "$version"

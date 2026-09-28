#!/usr/bin/env bash

# Runs the Native smoke binary against one broker under a leak checker, so the C
# shim's allocations are exercised through a real produce, consume, and commit
# round trip: the producer batch, its delivery slots, message headers, the
# per-client state, and the consumer and producer handles themselves.
#
# A checker only reports allocations that are unreachable at exit, which is what
# a missed free in the shim would look like.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
downstream_dir="$project_dir/tests/downstream"
version="0.1.0-LEAKCHECK"
platform="$(uname -s)"

if [[ "${1:-}" == "--run" ]]; then
  binary="${XKAFKA_SMOKE_BINARY:?the linked smoke binary was not passed through}"
  case "$platform" in
    Darwin) exec leaks --atExit -- "$binary" ;;
    Linux)
      export LD_LIBRARY_PATH="$XKAFKA_LIBRDKAFKA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      if [[ -z "${LSAN_SYMBOLIZER_PATH:-}" ]]; then
        if ! LSAN_SYMBOLIZER_PATH="$(command -v llvm-symbolizer)"; then
          echo "llvm-symbolizer is required for the Native leak check" >&2
          exit 2
        fi
      fi
      export LSAN_SYMBOLIZER_PATH
      export LSAN_OPTIONS="suppressions=$script_dir/native-leak-check.lsan:print_suppressions=1${LSAN_OPTIONS:+:$LSAN_OPTIONS}"
      exec "$binary"
      ;;
    *)      echo "no leak checker configured for $platform" >&2; exit 2 ;;
  esac
fi

if [[ "$platform" == Linux ]]; then
  export XKAFKA_NATIVE_LEAK_CHECK=1
fi

echo "Publishing $version locally"
(cd "$project_dir" && sbt --error "set ThisBuild/version := \"$version\"" publishLocal)

if [[ -z "${XKAFKA_LIBRDKAFKA_PREFIX:-}" ]]; then
  (cd "$project_dir" && sbt --error clientNative/prepareLibrdkafka)
  while IFS= read -r candidate; do
    if [[ -f "$candidate/include/librdkafka/rdkafka.h" && -f "$candidate/lib/librdkafka.a" ]]; then
      XKAFKA_LIBRDKAFKA_PREFIX="$candidate"
      break
    fi
  done < <(find "$project_dir/modules/client/native/target" -maxdepth 1 -type d -name 'librdkafka-*' | sort)
fi

: "${XKAFKA_LIBRDKAFKA_PREFIX:?could not locate the prepared librdkafka installation}"
export XKAFKA_LIBRDKAFKA_PREFIX
export XKAFKA_VERSION="$version"

# `print` reports what the link actually produced, so the path follows the Scala version the
# downstream build is on instead of naming one here for a bump to invalidate.
echo "Building the Native smoke binary"
binary=""
while IFS= read -r candidate; do
  candidate="${candidate%$'\r'}"
  if [[ -x "$candidate" ]]; then
    binary="$candidate"
  fi
done < <(cd "$downstream_dir" && sbt --no-colors --supershell=false --batch --error "print smokeNative/nativeLink")

if [[ ! -x "$binary" ]]; then
  echo "the Native link did not report an executable" >&2
  exit 1
fi

export XKAFKA_SMOKE_BINARY="$binary"

exec "$script_dir/with-kafka.sh" "$script_dir/native-leak-check.sh" --run

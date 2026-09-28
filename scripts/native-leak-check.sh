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

if [[ "${1:-}" == "--run" ]]; then
  binary="${XKAFKA_SMOKE_BINARY:?the linked smoke binary was not passed through}"
  case "$(uname -s)" in
    Darwin) exec leaks --atExit -- "$binary" ;;
    Linux)
      export LD_LIBRARY_PATH="$XKAFKA_LIBRDKAFKA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      exec valgrind --leak-check=full --error-exitcode=1 "$binary"
      ;;
    *)      echo "no leak checker configured for $(uname -s)" >&2; exit 2 ;;
  esac
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
binary="$(cd "$downstream_dir" && sbt --error "print smokeNative/nativeLink" | tr -d '\r' | tail -n 1)"

if [[ ! -x "$binary" ]]; then
  echo "the Native link reported '$binary', which is not an executable" >&2
  exit 1
fi

export XKAFKA_SMOKE_BINARY="$binary"

exec "$script_dir/with-kafka.sh" "$script_dir/native-leak-check.sh" --run

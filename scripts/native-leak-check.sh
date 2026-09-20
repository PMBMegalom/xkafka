#!/usr/bin/env bash

# Runs the Native smoke binary against one broker under a leak checker, so the C
# shim's allocations are exercised through a real produce, consume, and commit
# round trip: the producer batch, its delivery slots, message headers, the
# rebalance counter, and the consumer and producer handles themselves.
#
# A checker only reports allocations that are unreachable at exit, which is what
# a missed free in the shim would look like.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
downstream_dir="$project_dir/tests/downstream"
version="0.1.0-LEAKCHECK"

if [[ "${1:-}" == "--run" ]]; then
  binary="$downstream_dir/.native/target/scala-3.3.8/native/xkafka.downstream.DownstreamSmoke"
  case "$(uname -s)" in
    Darwin) exec leaks --atExit -- "$binary" ;;
    Linux)  exec valgrind --leak-check=full --error-exitcode=1 "$binary" ;;
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

echo "Building the Native smoke binary"
(cd "$downstream_dir" && sbt --error smokeNative/nativeLink)

exec "$script_dir/with-kafka.sh" "$script_dir/native-leak-check.sh" --run

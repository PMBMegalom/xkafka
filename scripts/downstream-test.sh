#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
downstream_dir="$project_dir/tests/downstream"

if [[ "${1:-}" == "--run" ]]; then
  echo "Testing the published JVM artifact"
  sbt smokeJVM/run
  echo "Testing the published Scala.js artifact"
  sbt smokeJS/run
  echo "Testing the published Scala Native artifact"
  sbt smokeNative/run
  exit
fi

: "${XKAFKA_VERSION:?XKAFKA_VERSION must name the published xkafka version to test}"

for attempt in $(seq 1 30); do
  if (cd "$downstream_dir" && sbt smokeJVM/update); then
    break
  fi
  if [[ "$attempt" -eq 30 ]]; then
    echo "published xkafka artifacts did not become available" >&2
    exit 1
  fi
  echo "published xkafka artifacts are not available yet; retrying in 10 seconds"
  sleep 10
done

if [[ -z "${XKAFKA_LIBRDKAFKA_PREFIX:-}" ]]; then
  (cd "$project_dir" && sbt clientNative/prepareLibrdkafka)
  while IFS= read -r candidate; do
    if [[ -f "$candidate/include/librdkafka/rdkafka.h" && -f "$candidate/lib/librdkafka.a" ]]; then
      XKAFKA_LIBRDKAFKA_PREFIX="$candidate"
      break
    fi
  done < <(find "$project_dir/modules/client/native/target" -maxdepth 1 -type d -name 'librdkafka-*' | sort)
fi

: "${XKAFKA_LIBRDKAFKA_PREFIX:?could not locate the prepared librdkafka installation}"
export XKAFKA_LIBRDKAFKA_PREFIX

exec "$script_dir/with-kafka.sh" --directory "$downstream_dir" "$script_dir/downstream-test.sh" --run

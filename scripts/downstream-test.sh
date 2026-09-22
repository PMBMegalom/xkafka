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

  # The two ways a Native application reaches librdkafka: the one the README promises needs no xkafka
  # settings, and the one that links the archive and names the libraries it was built against.
  echo "Testing the published Scala Native artifact against an installed librdkafka"
  (unset XKAFKA_LIBRDKAFKA_PREFIX XKAFKA_LIBRDKAFKA_STATIC; sbt smokeNative/run)

  echo "Testing the published Scala Native artifact against a static librdkafka"
  XKAFKA_LIBRDKAFKA_PREFIX="$XKAFKA_LIBRDKAFKA_STATIC_PREFIX" XKAFKA_LIBRDKAFKA_STATIC=1 sbt smokeNative/run
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

prepared=""
(cd "$project_dir" && sbt clientNative/prepareLibrdkafka)
while IFS= read -r candidate; do
  if [[ -f "$candidate/include/librdkafka/rdkafka.h" && -f "$candidate/lib/librdkafka.a" ]]; then
    prepared="$candidate"
    break
  fi
done < <(find "$project_dir/modules/client/native/target" -maxdepth 1 -type d -name 'librdkafka-*' | sort)

: "${prepared:?could not locate the prepared librdkafka installation}"

# The prepared prefix carries the shared library beside the archive, and a linker offered both takes the
# shared one. Staging the archive on its own is what makes the static case actually static.
staged="$project_dir/target/downstream-librdkafka-static"
rm -rf "$staged"
mkdir -p "$staged/lib"
ln -s "$prepared/include" "$staged/include"
cp "$prepared/lib/librdkafka.a" "$staged/lib/librdkafka.a"
export XKAFKA_LIBRDKAFKA_STATIC_PREFIX="$staged"

# Homebrew keeps OpenSSL keg-only, so a static link on macOS has to be told where it lives.
if [[ "$(uname -s)" == "Darwin" ]]; then
  search_paths=""
  for formula in openssl@3 zstd zlib; do
    prefix="$(brew --prefix "$formula" 2>/dev/null || true)"
    if [[ -n "$prefix" && -d "$prefix/lib" ]]; then
      search_paths="$search_paths -L$prefix/lib"
    fi
  done
  export XKAFKA_LIBRDKAFKA_SEARCH_PATHS="${search_paths# }"
fi

exec "$script_dir/with-kafka.sh" --directory "$downstream_dir" "$script_dir/downstream-test.sh" --run

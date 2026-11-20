#!/usr/bin/env bash
# Measures produce, consume and streams rates on each backend against one local broker, which scripts/with-kafka.sh starts.
#
#   scripts/benchmark.sh [jvm|js|native|all]
#
# XKAFKA_VERSION names the xkafka release measured (default 0.8.0); XKAFKA_BENCH_RECORDS, XKAFKA_BENCH_VALUE_BYTES, XKAFKA_BENCH_KEYS and
# XKAFKA_BENCH_PARTITIONS size the run.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
benchmarks_dir="$project_dir/benchmarks"

commands=()
case "${1:-all}" in
  jvm)    commands=(benchJVM/run) ;;
  js)     commands=(benchJS/run) ;;
  native) commands=(benchNative/run) ;;
  all)    commands=(benchJVM/run benchJS/run benchNative/run) ;;
  *)      echo "usage: $0 [jvm|js|native|all]" >&2; exit 2 ;;
esac

# Scala Native, and the C program that measures librdkafka used directly for Scala.js and Scala Native, link the librdkafka archive the main build
# prepares, and the libraries it was built against.
librdkafka_baseline=false
if [[ " ${commands[*]} " == *" benchNative/run "* || " ${commands[*]} " == *" benchJS/run "* ]]; then
  librdkafka_baseline=true
  (cd "$project_dir" && sbt clientNative/prepareLibrdkafka)
  prepared="$(find "$project_dir/modules/client/native/target" -maxdepth 1 -type d -name 'librdkafka-*' | sort | while read -r candidate; do
    if [[ -f "$candidate/include/librdkafka/rdkafka.h" && -f "$candidate/lib/librdkafka.a" ]]; then echo "$candidate"; fi
  done | head -1)"
  : "${prepared:?could not locate the prepared librdkafka installation}"
  staged="$project_dir/target/benchmark-librdkafka-static"
  rm -rf "$staged" && mkdir -p "$staged/lib"
  ln -s "$prepared/include" "$staged/include"
  cp "$prepared/lib/librdkafka.a" "$staged/lib/librdkafka.a"
  export XKAFKA_LIBRDKAFKA_PREFIX="$staged"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    search_paths=""
    include_paths=""
    for formula in openssl@3 zstd zlib; do
      prefix="$(brew --prefix "$formula" 2>/dev/null || true)"
      if [[ -n "$prefix" && -d "$prefix/lib" ]]; then
        search_paths="$search_paths -L$prefix/lib"
        include_paths="$include_paths -I$prefix/include"
      fi
    done
    export XKAFKA_LIBRDKAFKA_SEARCH_PATHS="${search_paths# }"
    export XKAFKA_INCLUDE_PATHS="${include_paths# }"
    openssl="$(brew --prefix openssl@3 2>/dev/null || true)"
    if [[ -n "$openssl" ]]; then export XKAFKA_OPENSSL_LIBRARIES="$openssl/lib/libssl.dylib $openssl/lib/libcrypto.dylib"; fi
  fi
fi

run="sbt ${commands[*]}"
if [[ "$librdkafka_baseline" == true ]]; then
  baseline="$project_dir/target/benchmark-librdkafka"
  libraries=(-lssl -lcrypto)
  if [[ -n "${XKAFKA_OPENSSL_LIBRARIES:-}" ]]; then read -r -a libraries <<<"$XKAFKA_OPENSSL_LIBRARIES"; fi
  read -r -a search <<<"${XKAFKA_LIBRDKAFKA_SEARCH_PATHS:-}"
  read -r -a includes <<<"${XKAFKA_INCLUDE_PATHS:-}"
  cc -O2 -o "$baseline" "$benchmarks_dir/librdkafka/bench.c" -I"$staged/include" ${includes[@]+"${includes[@]}"} -L"$staged/lib" ${search[@]+"${search[@]}"} \
    -lrdkafka ${libraries[@]+"${libraries[@]}"} -lz -lzstd -lpthread -lm
  run="$run && $baseline"
fi

exec "$script_dir/with-kafka.sh" --directory "$benchmarks_dir" bash -c "$run"

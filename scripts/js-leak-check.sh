#!/usr/bin/env bash

# Runs every asynchronous call of the JavaScript backend's library against one broker under a leak checker:
# the jobs that start them, the strings they copy, and the shim and librdkafka work each one does on its own
# thread. The calls come from js-leak-check.c, linked against the library the npm package ships.
#
# A leak checker cannot watch these calls inside Node: what is allocated during a call koffi makes is never
# reported there, so a job left behind would go unseen.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
output="$project_dir/target/js-leak-check"
harness="$output/js-leak-check"
platform="$(uname -s)"

if [[ "${1:-}" == "--run" ]]; then
  bootstrap="${XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS:?the broker address was not passed through}"
  case "$platform" in
    Darwin) exec leaks --atExit -- "$harness" "$bootstrap" ;;
    Linux)  exec "$harness" "$bootstrap" ;;
  esac
fi

case "$platform" in
  Darwin) sanitizer="" ;;
  Linux)  sanitizer="-fsanitize=leak" ;;
  *)      echo "no leak checker configured for $platform" >&2; exit 2 ;;
esac

# `print` reports the library the build produced, so the path is never named here.
library=""
while IFS= read -r candidate; do
  candidate="${candidate%$'\r'}"
  if [[ -f "$candidate" ]]; then
    library="$candidate"
  fi
done < <(cd "$project_dir" && sbt --no-colors --supershell=false --batch --error "print clientJS/sharedLibrdkafka")

if [[ ! -f "$library" ]]; then
  echo "the build did not report the library" >&2
  exit 1
fi

mkdir -p "$output"
cc -std=c11 -g -Wall -Wextra -Werror $sanitizer -o "$harness" "$script_dir/js-leak-check.c" "$library" \
  -Wl,-rpath,"$(dirname "$library")" -lpthread

exec "$script_dir/with-kafka.sh" "$script_dir/js-leak-check.sh" --run

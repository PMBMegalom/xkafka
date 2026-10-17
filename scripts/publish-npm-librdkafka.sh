#!/usr/bin/env bash

# Publishes the @xkafka/librdkafka packages in a directory, each one only where npm does not have its
# version yet. Every platform package goes first, so the umbrella package never names one that is missing.
#
# usage: publish-npm-librdkafka.sh directory

set -euo pipefail

directory="$1"
expected_platforms=6

field() {
  node -p 'require(require("path").resolve(process.argv[1], "package.json"))[process.argv[2]]' "$1" "$2"
}

publish() {
  local package="$1"
  local name version
  name="$(field "$package" name)"
  version="$(field "$package" version)"
  if npm view "$name@$version" version >/dev/null 2>&1; then
    echo "$name@$version is already published"
  else
    # The versions name a librdkafka release and a shim build, which npm reads as prereleases, so the tag is given explicitly.
    (cd "$package" && npm publish --access public --tag latest)
  fi
}

platforms=("$directory"/librdkafka-*/)
if [[ "${#platforms[@]}" -ne "$expected_platforms" ]]; then
  echo "expected $expected_platforms platform packages in $directory, found ${#platforms[@]}" >&2
  exit 1
fi

for package in "${platforms[@]}"; do
  publish "${package%/}"
done
publish "$directory/librdkafka"

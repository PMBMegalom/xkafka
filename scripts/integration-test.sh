#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# With --streams, the streams engine's tests run against the brokers of each supported release, beside the Java
# Streams application they join, which runs in the background of the same sbt session and stops on exit. Without
# it, every other broker test runs.
streams=false
if [[ "${1:-}" == "--streams" ]]; then
  streams=true
  shift
fi

if [[ "$streams" == true ]]; then
  suites='xkafka.internal.streams.GroupMemberBrokerSuite xkafka.internal.streams.RestoreSuite'
else
  suites='xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite xkafka.KafkaSecuritySuite xkafka.PartitionPausingSuite xkafka.KafkaClusterSuite xkafka.StreamsDriverSuite xkafka.internal.group.GroupSocketSuite xkafka.internal.group.GroupConnectionSuite'
fi

# With a backend named (jvm, js or native), only that one runs; with none, all three run one after the other.
case "${1:-all}" in
  jvm)    backends=(jvm) ;;
  js)     backends=(js) ;;
  native) backends=(native) ;;
  all)    backends=(jvm js native) ;;
  *)      echo "usage: $0 [--streams] [jvm|js|native]" >&2; exit 2 ;;
esac

commands=()
if [[ "$streams" == true ]]; then
  commands+=("streamsFixture/bgRun ${backends[*]}")
fi
for backend in "${backends[@]}"; do
  case "$backend" in
    jvm)    commands+=("clientJVM/Test/testOnly $suites") ;;
    js)     commands+=("clientJS/Test/testOnly $suites") ;;
    native) commands+=("clientNative/Test/testOnly $suites") ;;
  esac
done

if [[ "$streams" == true ]]; then
  exec "$script_dir/with-kafka.sh" --streams sbt "${commands[@]}"
else
  exec "$script_dir/with-kafka.sh" sbt "${commands[@]}"
fi

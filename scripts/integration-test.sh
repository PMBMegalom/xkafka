#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

suites='xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite xkafka.KafkaSecuritySuite xkafka.PartitionPausingSuite xkafka.KafkaClusterSuite xkafka.StreamsDriverSuite xkafka.internal.group.GroupSocketSuite xkafka.internal.group.GroupConnectionSuite xkafka.internal.streams.GroupMemberBrokerSuite xkafka.internal.streams.RestoreSuite'

# With a backend named (jvm, js or native), only that one runs; with none, all three run one after the other.
# The Java Streams application the streams group tests join runs in the background of the same sbt session, which stops it on exit.
case "${1:-all}" in
  jvm)    commands=("streamsFixture/bgRun jvm" "clientJVM/Test/testOnly $suites") ;;
  js)     commands=("streamsFixture/bgRun js" "clientJS/Test/testOnly $suites") ;;
  native) commands=("streamsFixture/bgRun native" "clientNative/Test/testOnly $suites") ;;
  all)    commands=("streamsFixture/bgRun jvm js native" "clientJVM/Test/testOnly $suites" "clientJS/Test/testOnly $suites" "clientNative/Test/testOnly $suites") ;;
  *)      echo "usage: $0 [jvm|js|native]" >&2; exit 2 ;;
esac

exec "$script_dir/with-kafka.sh" sbt "${commands[@]}"

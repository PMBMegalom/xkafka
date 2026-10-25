#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

suites='xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite xkafka.KafkaSecuritySuite xkafka.PartitionPausingSuite xkafka.KafkaClusterSuite'

# With a backend named (jvm, js or native), only that one runs; with none, all three run one after the other.
case "${1:-all}" in
  jvm)    commands=("clientJVM/Test/testOnly $suites") ;;
  js)     commands=("clientJS/Test/testOnly $suites") ;;
  native) commands=("clientNative/Test/testOnly $suites") ;;
  all)    commands=("clientJVM/Test/testOnly $suites" "clientJS/Test/testOnly $suites" "clientNative/Test/testOnly $suites") ;;
  *)      echo "usage: $0 [jvm|js|native]" >&2; exit 2 ;;
esac

exec "$script_dir/with-kafka.sh" sbt "${commands[@]}"

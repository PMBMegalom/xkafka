#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

suites='xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite xkafka.KafkaSecuritySuite'

exec "$script_dir/with-kafka.sh" sbt \
  "clientJVM/Test/testOnly $suites" \
  "clientJS/Test/testOnly $suites" \
  "clientNative/Test/testOnly $suites"

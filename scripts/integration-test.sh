#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

suites='xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite xkafka.KafkaSecuritySuite'

# The librdkafka backends also carry what the kernel's partition router needs from them.
shared='xkafka.PartitionPausingSuite'

exec "$script_dir/with-kafka.sh" sbt \
  "clientJVM/Test/testOnly $suites" \
  "clientJS/Test/testOnly $suites $shared" \
  "clientNative/Test/testOnly $suites $shared"

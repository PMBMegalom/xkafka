#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$script_dir/with-kafka.sh" sbt \
  'clientJVM/Test/testOnly xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite' \
  'clientJS/Test/testOnly xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite' \
  'clientNative/Test/testOnly xkafka.KafkaIntegrationSuite xkafka.KafkaConformanceSuite'

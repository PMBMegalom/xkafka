#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$script_dir/with-kafka.sh" sbt \
  'clientJVM/Test/testOnly xkafka.KafkaIntegrationSuite' \
  'clientJS/Test/testOnly xkafka.KafkaIntegrationSuite' \
  'clientNative/Test/testOnly xkafka.KafkaIntegrationSuite'

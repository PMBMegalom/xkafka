#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
container_name="xkafka-integration-$$"
host_port="${XKAFKA_INTEGRATION_PORT:-19092}"

cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]]; then
    docker logs --tail 200 "$container_name" || true
  fi
  docker rm --force --volumes "$container_name" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker run --detach --rm \
  --name "$container_name" \
  --publish "$host_port:9092" \
  --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1" \
  --health-interval 2s \
  --health-timeout 5s \
  --health-retries 30 \
  --health-start-period 10s \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  --env KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT://:19092,PLAINTEXT_HOST://:9092 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,PLAINTEXT_HOST://127.0.0.1:"$host_port" \
  --env KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  --env KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 \
  --env KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  --env CLUSTER_ID=4L6g3nShT-eMCtK--X86sw \
  apache/kafka:4.3.1 >/dev/null

for _ in $(seq 1 60); do
  status="$(docker inspect --format '{{.State.Health.Status}}' "$container_name")"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  if [[ "$status" == "unhealthy" ]]; then
    docker logs "$container_name"
    exit 1
  fi
  sleep 2
done

if [[ "$(docker inspect --format '{{.State.Health.Status}}' "$container_name")" != "healthy" ]]; then
  docker logs "$container_name"
  exit 1
fi

export XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS="127.0.0.1:$host_port"

cd "$project_dir"
sbt \
  'clientJVM/Test/testOnly xkafka.KafkaIntegrationSuite' \
  'clientJS/Test/testOnly xkafka.KafkaIntegrationSuite' \
  'clientNative/Test/testOnly xkafka.KafkaIntegrationSuite'

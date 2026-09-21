#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
container_name="xkafka-integration-$$"
host_port="${XKAFKA_INTEGRATION_PORT:-19092}"
tls_port="${XKAFKA_INTEGRATION_TLS_PORT:-19093}"
sasl_port="${XKAFKA_INTEGRATION_SASL_PORT:-19094}"
username="xkafka"
password="xkafka-secret"
secrets_dir="$project_dir/target/kafka-secrets-$$"
working_directory="$project_dir"

if [[ "${1:-}" == "--directory" ]]; then
  working_directory="$2"
  shift 2
fi

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 [--directory path] command [argument ...]" >&2
  exit 2
fi

cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]]; then
    docker logs --tail 200 "$container_name" || true
  fi
  docker rm --force --volumes "$container_name" >/dev/null 2>&1 || true
  rm -rf "$secrets_dir"
  exit "$status"
}
trap cleanup EXIT

# The broker presents one certificate on both secure listeners. Clients verify the address they
# connected to against it, so the certificate names both localhost and 127.0.0.1. The listeners
# advertise the address, because the name resolves to a loopback address on each stack and Docker
# publishes only the IPv4 one.
mkdir -p "$secrets_dir"
openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
  -keyout "$secrets_dir/ca.key" -out "$secrets_dir/ca.pem" \
  -subj "/CN=xkafka test certificate authority" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "$secrets_dir/broker.key" -out "$secrets_dir/broker.csr" \
  -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in "$secrets_dir/broker.csr" -sha256 -days 2 \
  -CA "$secrets_dir/ca.pem" -CAkey "$secrets_dir/ca.key" -CAcreateserial \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
  -out "$secrets_dir/broker.crt" >/dev/null 2>&1
cat "$secrets_dir/broker.key" "$secrets_dir/broker.crt" > "$secrets_dir/broker.pem"
chmod a+r "$secrets_dir"/*.pem

docker run --detach --rm \
  --name "$container_name" \
  --publish "$host_port:9092" \
  --publish "127.0.0.1:$tls_port:9093" \
  --publish "127.0.0.1:$sasl_port:9094" \
  --volume "$secrets_dir:/etc/kafka/secrets:ro" \
  --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1" \
  --health-interval 2s \
  --health-timeout 5s \
  --health-retries 30 \
  --health-start-period 10s \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,TLS:SSL,SASLTLS:SASL_SSL \
  --env KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT://:19092,PLAINTEXT_HOST://:9092,TLS://:9093,SASLTLS://:9094 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,PLAINTEXT_HOST://127.0.0.1:"$host_port",TLS://127.0.0.1:"$tls_port",SASLTLS://127.0.0.1:"$sasl_port" \
  --env KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
  --env KAFKA_SSL_KEYSTORE_TYPE=PEM \
  --env KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/broker.pem \
  --env KAFKA_SSL_TRUSTSTORE_TYPE=PEM \
  --env KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.pem \
  --env KAFKA_SSL_CLIENT_AUTH=none \
  --env KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256 \
  --env KAFKA_LISTENER_NAME_SASLTLS_SCRAM___SHA___256_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required;' \
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

# SCRAM credentials live in the metadata log, so the user is created once the broker is serving.
docker exec "$container_name" /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:19092 \
  --alter --add-config "SCRAM-SHA-256=[password=$password]" \
  --entity-type users --entity-name "$username" >/dev/null

export XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS="127.0.0.1:$host_port"
export XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS="127.0.0.1:$tls_port"
export XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS="127.0.0.1:$sasl_port"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY="$secrets_dir/ca.pem"
XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM="$(cat "$secrets_dir/ca.pem")"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM
export XKAFKA_SECURITY_USERNAME="$username"
export XKAFKA_SECURITY_PASSWORD="$password"

cd "$working_directory"
"$@"

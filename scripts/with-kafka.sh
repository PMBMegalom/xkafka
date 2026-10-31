#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
container_name="xkafka-integration-$$"
host_port="${XKAFKA_INTEGRATION_PORT:-19092}"
tls_port="${XKAFKA_INTEGRATION_TLS_PORT:-19093}"
sasl_port="${XKAFKA_INTEGRATION_SASL_PORT:-19094}"
mtls_port="${XKAFKA_INTEGRATION_MTLS_PORT:-19095}"
wrong_name_port="${XKAFKA_INTEGRATION_WRONG_NAME_PORT:-19099}"
partitioned_topic="xkafka-partitioned"
# A cluster of three brokers beside the single one, for what needs replicas on more than one broker.
cluster_name="xkafka-cluster-$$"
cluster_ports=("${XKAFKA_CLUSTER_PORT_1:-19096}" "${XKAFKA_CLUSTER_PORT_2:-19097}" "${XKAFKA_CLUSTER_PORT_3:-19098}")
election_topic="xkafka-election"
# Brokers of the oldest and newest releases xkafka's streams groups support, beside the main broker's, for the streams group tests.
streams_name="xkafka-streams-$$"
streams_releases=(4.2.2 4.4.0)
streams_ports=("${XKAFKA_STREAMS_PORT_1:-19100}" "${XKAFKA_STREAMS_PORT_2:-19101}")
username="xkafka"
password="xkafka-secret"
key_password="xkafka-key-secret"
secrets_dir="$project_dir/target/kafka-secrets-$$"
logs_dir="$project_dir/target/broker-logs"
working_directory="$project_dir"
# With --streams, the brokers the streams tests need start in place of the cluster: the main broker and those of
# the oldest and newest releases xkafka's streams groups support.
streams=false

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --directory) working_directory="$2"; shift 2 ;;
    --streams)   streams=true; shift ;;
    *)           break ;;
  esac
done

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 [--directory path] [--streams] command [argument ...]" >&2
  exit 2
fi

cleanup() {
  status=$?
  # A failure keeps each broker's whole log in target/broker-logs, for a test that failed long before the tails printed here.
  if [[ "$status" -ne 0 ]]; then
    mkdir -p "$logs_dir"
    docker logs "$container_name" >"$logs_dir/broker.log" 2>&1 || true
    docker logs --tail 200 "$container_name" || true
  fi
  docker rm --force --volumes "$container_name" >/dev/null 2>&1 || true
  for node in 1 2 3; do
    if [[ "$status" -ne 0 ]]; then
      docker logs "$cluster_name-$node" >"$logs_dir/cluster-$node.log" 2>&1 || true
      docker logs --tail 50 "$cluster_name-$node" || true
    fi
    docker rm --force --volumes "$cluster_name-$node" >/dev/null 2>&1 || true
  done
  docker network rm "$cluster_name" >/dev/null 2>&1 || true
  for release in "${streams_releases[@]}"; do
    if [[ "$status" -ne 0 ]]; then
      docker logs "$streams_name-$release" >"$logs_dir/streams-$release.log" 2>&1 || true
      docker logs --tail 50 "$streams_name-$release" || true
    fi
    docker rm --force --volumes "$streams_name-$release" >/dev/null 2>&1 || true
  done
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
# A broker certificate from the same authority naming neither address, for the listener a client checking the broker's name must refuse.
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "$secrets_dir/wrong-name.key" -out "$secrets_dir/wrong-name.csr" \
  -subj "/CN=wrong-name.invalid" >/dev/null 2>&1
openssl x509 -req -in "$secrets_dir/wrong-name.csr" -sha256 -days 2 \
  -CA "$secrets_dir/ca.pem" -CAkey "$secrets_dir/ca.key" -CAcreateserial \
  -extfile <(printf 'subjectAltName=DNS:wrong-name.invalid\nextendedKeyUsage=serverAuth\n') \
  -out "$secrets_dir/wrong-name.crt" >/dev/null 2>&1
cat "$secrets_dir/wrong-name.key" "$secrets_dir/wrong-name.crt" > "$secrets_dir/wrong-name.pem"
# A client certificate from the same authority, for the listener that requires one. Its key is PKCS#8,
# the only form the Java client reads, once as it is and once encrypted with a scheme Java 17 reads.
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "$secrets_dir/client.key" -out "$secrets_dir/client.csr" \
  -subj "/CN=xkafka test client" >/dev/null 2>&1
openssl x509 -req -in "$secrets_dir/client.csr" -sha256 -days 2 \
  -CA "$secrets_dir/ca.pem" -CAkey "$secrets_dir/ca.key" -CAcreateserial \
  -extfile <(printf 'extendedKeyUsage=clientAuth\n') \
  -out "$secrets_dir/client.crt" >/dev/null 2>&1
openssl pkcs8 -topk8 -v1 PBE-SHA1-3DES -in "$secrets_dir/client.key" \
  -out "$secrets_dir/client-encrypted.key" -passout "pass:$key_password" >/dev/null 2>&1
cat "$secrets_dir/client.key" "$secrets_dir/client.crt" > "$secrets_dir/client.pem"
cat "$secrets_dir/client-encrypted.key" "$secrets_dir/client.crt" > "$secrets_dir/client-encrypted.pem"
# The same key encrypted with PBES2, the scheme openssl uses by default.
openssl pkcs8 -topk8 -in "$secrets_dir/client.key" \
  -out "$secrets_dir/client-pbes2.key" -passout "pass:$key_password" >/dev/null 2>&1
cat "$secrets_dir/client-pbes2.key" "$secrets_dir/client.crt" > "$secrets_dir/client-pbes2.pem"
# The authority as a directory of certificates: once as a plain file, once under OpenSSL's hashed name.
mkdir -p "$secrets_dir/authorities-plain" "$secrets_dir/authorities-hashed"
cp "$secrets_dir/ca.pem" "$secrets_dir/authorities-plain/ca.pem"
cp "$secrets_dir/ca.pem" "$secrets_dir/authorities-hashed/$(openssl x509 -hash -noout -in "$secrets_dir/ca.pem").0"
chmod a+r "$secrets_dir"/*.pem "$secrets_dir"/authorities-*/*
chmod a+rx "$secrets_dir"/authorities-*

docker run --detach --rm \
  --name "$container_name" \
  --publish "$host_port:9092" \
  --publish "127.0.0.1:$tls_port:9093" \
  --publish "127.0.0.1:$sasl_port:9094" \
  --publish "127.0.0.1:$mtls_port:9095" \
  --publish "127.0.0.1:$wrong_name_port:9099" \
  --volume "$secrets_dir:/etc/kafka/secrets:ro" \
  --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1" \
  --health-interval 2s \
  --health-timeout 5s \
  --health-retries 30 \
  --health-start-period 10s \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,TLS:SSL,SASLTLS:SASL_SSL,MTLS:SSL,WRONGNAME:SSL \
  --env KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT://:19092,PLAINTEXT_HOST://:9092,TLS://:9093,SASLTLS://:9094,MTLS://:9095,WRONGNAME://:9099 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,PLAINTEXT_HOST://127.0.0.1:"$host_port",TLS://127.0.0.1:"$tls_port",SASLTLS://127.0.0.1:"$sasl_port",MTLS://127.0.0.1:"$mtls_port",WRONGNAME://127.0.0.1:"$wrong_name_port" \
  --env KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
  --env KAFKA_SSL_KEYSTORE_TYPE=PEM \
  --env KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/broker.pem \
  --env KAFKA_SSL_TRUSTSTORE_TYPE=PEM \
  --env KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.pem \
  --env KAFKA_SSL_CLIENT_AUTH=none \
  --env KAFKA_LISTENER_NAME_MTLS_SSL_CLIENT_AUTH=required \
  --env KAFKA_LISTENER_NAME_WRONGNAME_SSL_KEYSTORE_TYPE=PEM \
  --env KAFKA_LISTENER_NAME_WRONGNAME_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/wrong-name.pem \
  --env KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256,SCRAM-SHA-512,PLAIN \
  --env KAFKA_LISTENER_NAME_SASLTLS_SCRAM___SHA___256_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required;' \
  --env KAFKA_LISTENER_NAME_SASLTLS_SCRAM___SHA___512_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required;' \
  --env KAFKA_LISTENER_NAME_SASLTLS_PLAIN_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required user_$username=\"$password\";" \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  --env KAFKA_GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS=500 \
  --env KAFKA_GROUP_STREAMS_HEARTBEAT_INTERVAL_MS=500 \
  --env KAFKA_GROUP_STREAMS_NUM_STANDBY_REPLICAS=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  --env KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 \
  --env KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  --env CLUSTER_ID=4L6g3nShT-eMCtK--X86sw \
  apache/kafka:4.3.1 >/dev/null

if [[ "$streams" == false ]]; then
  # Each broker of the cluster is also one of its three controllers. Leaders are moved back to the preferred
  # replica only by an election that asks for it, so a partition led by another replica stays that way.
  docker network create "$cluster_name" >/dev/null
  for node in 1 2 3; do
    docker run --detach --rm \
      --name "$cluster_name-$node" \
      --network "$cluster_name" \
      --publish "127.0.0.1:${cluster_ports[$((node - 1))]}:9092" \
      --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1" \
      --health-interval 2s \
      --health-timeout 5s \
      --health-retries 30 \
      --health-start-period 10s \
      --env KAFKA_NODE_ID="$node" \
      --env KAFKA_PROCESS_ROLES=broker,controller \
      --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT \
      --env KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,HOST://:9092 \
      --env KAFKA_ADVERTISED_LISTENERS=INTERNAL://"$cluster_name-$node":19092,HOST://127.0.0.1:"${cluster_ports[$((node - 1))]}" \
      --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
      --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      --env KAFKA_CONTROLLER_QUORUM_VOTERS="1@$cluster_name-1:29093,2@$cluster_name-2:29093,3@$cluster_name-3:29093" \
      --env KAFKA_AUTO_LEADER_REBALANCE_ENABLE=false \
      --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      --env KAFKA_HEAP_OPTS="-Xmx384m -Xms384m" \
      --env KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
      --env CLUSTER_ID=5M7h4oTiU-fNDuL--Y97tx \
      apache/kafka:4.3.1 >/dev/null
  done
else
  # Streams group members heartbeat every half second on these brokers and the main one, so the tests see a rebalance within seconds, and
  # each task has a standby wherever a group has a second member to keep it.
  for index in "${!streams_releases[@]}"; do
    release="${streams_releases[$index]}"
    docker run --detach --rm \
      --name "$streams_name-$release" \
      --publish "127.0.0.1:${streams_ports[$index]}:9092" \
      --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1" \
      --health-interval 2s \
      --health-timeout 5s \
      --health-retries 30 \
      --health-start-period 10s \
      --env KAFKA_NODE_ID=1 \
      --env KAFKA_PROCESS_ROLES=broker,controller \
      --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT \
      --env KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,HOST://:9092 \
      --env KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19092,HOST://127.0.0.1:"${streams_ports[$index]}" \
      --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
      --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
      --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      --env KAFKA_GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS=500 \
      --env KAFKA_GROUP_STREAMS_HEARTBEAT_INTERVAL_MS=500 \
      --env KAFKA_GROUP_STREAMS_NUM_STANDBY_REPLICAS=1 \
      --env KAFKA_HEAP_OPTS="-Xmx384m -Xms384m" \
      --env KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
      --env CLUSTER_ID=6N8i5pUjV-gOEvM--Z08uy \
      "apache/kafka:$release" >/dev/null
  done
fi

await_healthy() {
  for _ in $(seq 1 60); do
    status="$(docker inspect --format '{{.State.Health.Status}}' "$1")"
    if [[ "$status" == "healthy" ]]; then
      return 0
    fi
    if [[ "$status" == "unhealthy" ]]; then
      docker logs "$1"
      exit 1
    fi
    sleep 2
  done
  docker logs "$1"
  exit 1
}

await_healthy "$container_name"
if [[ "$streams" == false ]]; then
  for node in 1 2 3; do
    await_healthy "$cluster_name-$node"
  done
else
  for release in "${streams_releases[@]}"; do
    await_healthy "$streams_name-$release"
  done
fi

# SCRAM credentials live in the metadata log, so the user is created once the broker is serving, one
# mechanism per call since the broker refuses two changes to a user's credentials in one request.
for mechanism in SCRAM-SHA-256 SCRAM-SHA-512; do
  docker exec "$container_name" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:19092 \
    --alter --add-config "$mechanism=[password=$password]" \
    --entity-type users --entity-name "$username" >/dev/null
done

# Topics are created on demand with one partition each, so a rebalance between two consumers needs
# one that was made with more.
docker exec "$container_name" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create --topic "$partitioned_topic" --partitions 2 --replication-factor 1 >/dev/null

if [[ "$streams" == false ]]; then
  # One topic for each backend, whose partition broker 1 leads while broker 2 is its preferred replica: made on
  # brokers 1, 2 and 3, then given the same replicas with broker 2 first. Reordering moves no leader.
  reassignment='{"version":1,"partitions":['
  for backend in jvm js native; do
    docker exec "$cluster_name-1" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:19092 \
      --create --topic "$election_topic-$backend" --replica-assignment 1:2:3 >/dev/null
    reassignment+="{\"topic\":\"$election_topic-$backend\",\"partition\":0,\"replicas\":[2,1,3]},"
  done
  reassignment="${reassignment%,}]}"
  printf '%s' "$reassignment" | docker exec --interactive "$cluster_name-1" sh -c 'cat > /tmp/reassignment.json'
  docker exec "$cluster_name-1" /opt/kafka/bin/kafka-reassign-partitions.sh \
    --bootstrap-server localhost:19092 --reassignment-json-file /tmp/reassignment.json --execute >/dev/null
  for _ in $(seq 1 30); do
    if ! docker exec "$cluster_name-1" /opt/kafka/bin/kafka-reassign-partitions.sh \
      --bootstrap-server localhost:19092 --reassignment-json-file /tmp/reassignment.json --verify | grep -q 'in progress'; then
      break
    fi
    sleep 1
  done
  export XKAFKA_CLUSTER_BOOTSTRAP_SERVERS="127.0.0.1:${cluster_ports[0]},127.0.0.1:${cluster_ports[1]},127.0.0.1:${cluster_ports[2]}"
  export XKAFKA_CLUSTER_ELECTION_TOPIC="$election_topic"
else
  export XKAFKA_STREAMS_BROKERS="${streams_releases[0]}=127.0.0.1:${streams_ports[0]},4.3.1=127.0.0.1:$host_port,${streams_releases[1]}=127.0.0.1:${streams_ports[1]}"
fi

export XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS="127.0.0.1:$host_port"
export XKAFKA_INTEGRATION_PARTITIONED_TOPIC="$partitioned_topic"
export XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS="127.0.0.1:$tls_port"
export XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS="127.0.0.1:$sasl_port"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY="$secrets_dir/ca.pem"
XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM="$(cat "$secrets_dir/ca.pem")"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM
export XKAFKA_SECURITY_MTLS_BOOTSTRAP_SERVERS="127.0.0.1:$mtls_port"
export XKAFKA_SECURITY_WRONG_NAME_BOOTSTRAP_SERVERS="127.0.0.1:$wrong_name_port"
export XKAFKA_SECURITY_CLIENT_PEM="$secrets_dir/client.pem"
export XKAFKA_SECURITY_CLIENT_ENCRYPTED_PEM="$secrets_dir/client-encrypted.pem"
export XKAFKA_SECURITY_CLIENT_PBES2_PEM="$secrets_dir/client-pbes2.pem"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PLAIN_DIRECTORY="$secrets_dir/authorities-plain"
export XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_HASHED_DIRECTORY="$secrets_dir/authorities-hashed"
XKAFKA_SECURITY_CLIENT_CERTIFICATE="$(cat "$secrets_dir/client.crt")"
export XKAFKA_SECURITY_CLIENT_CERTIFICATE
XKAFKA_SECURITY_CLIENT_KEY="$(cat "$secrets_dir/client.key")"
export XKAFKA_SECURITY_CLIENT_KEY
export XKAFKA_SECURITY_CLIENT_KEY_PASSWORD="$key_password"
export XKAFKA_SECURITY_USERNAME="$username"
export XKAFKA_SECURITY_PASSWORD="$password"

cd "$working_directory"
"$@"

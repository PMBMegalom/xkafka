# Contributing

## Running the tests

```sh
sbt test
```

That runs the ordinary JVM, Scala.js, and Scala Native suites. Use Node.js 24
for Scala.js, as `.nvmrc` records. Tests which need a broker or a schema
registry are skipped unless the environment names one, which the scripts below
do.

The Avro, JSON Schema and Protobuf suites on the JVM also check what xkafka
writes and reads against Apache Avro and Confluent's own serializers, which
come from Confluent's Maven repository.

## Broker-backed tests

```sh
scripts/integration-test.sh
```

The script starts, in Docker:

- one pinned Kafka broker, with plaintext, TLS, mutual TLS and SASL_SSL
  listeners;
- a cluster of three brokers, for what needs replicas on more than one broker,
  such as leader elections;
- a Confluent Schema Registry that keeps its schemas on the single broker.

It runs the same suites against all three backends, then removes the brokers,
the registry and their volumes. When a run fails, each container's log is kept
in `target/broker-logs`. Name a backend, `jvm`, `js` or `native`, to run only
that one, as CI does with each on a runner of its own:

```sh
scripts/integration-test.sh native
```

The suites it runs include:

- `KafkaIntegrationSuite` covers produce, consume, and commit round trips.
- `KafkaConformanceSuite` asserts behaviour every backend is expected to share,
  identically on each one.
- `KafkaSecuritySuite` covers TLS and SASL.
- `KafkaClusterSuite` covers what needs the three-broker cluster.
- `RegistryBrokerSuite` writes and reads Avro, JSON Schema and Protobuf through
  the real registry. It runs on the JVM and Scala.js only: it reaches the
  registry through Ember, which on Scala Native needs s2n-tls, a library Ubuntu
  does not package.

A case listed as divergent in the conformance suite is marked as expected to
fail on the named backends. Fixing the backend makes the marked case fail, so a
marker cannot outlive the divergence it records.

To run other commands against the same brokers and registry, such as a single
suite, use `scripts/with-kafka.sh`, which starts them, exports their addresses,
runs the command and cleans up:

```sh
scripts/with-kafka.sh sbt "schemaRegistryHttp4sJVM/Test/testOnly xkafka.schemaregistry.http4s.RegistryBrokerSuite"
```

The streams engine's tests run separately, against a broker of each Kafka
release it supports and beside a Java Kafka Streams application, as CI does in
a job of their own:

```sh
scripts/integration-test.sh --streams
```

## librdkafka for the Native build

The build downloads librdkafka, verifies its SHA-256 checksum, builds it, and
links it statically for the Native tests. To prepare it explicitly:

```sh
sbt clientNative/prepareLibrdkafka
```

A prebuilt static installation can be supplied instead:

```sh
XKAFKA_LIBRDKAFKA_PREFIX=/path/to/librdkafka-prefix sbt test
```

The prefix must contain `include/librdkafka/rdkafka.h` and `lib/librdkafka.a`.

## RocksDB for the Scala.js and Native builds

The build downloads the pinned `rocksdbjni` jar once, verifies its checksum, and
extracts the RocksDB library for this platform, which the Scala.js and Native
tests of stores kept on disk use.

## The C shims

```sh
scripts/native-leak-check.sh
```

Runs the Native smoke binary against a broker under `leaks` on macOS or
LeakSanitizer on Linux, so the shim's allocations are exercised through a real
round trip.

```sh
scripts/js-leak-check.sh
```

Runs every asynchronous call of the Scala.js library against a broker under the
same leak checkers, through a small C program linked against the library the
npm package ships.

```sh
sbt clientJS/rocksdbShimTest
```

Builds the RocksDB shim and tests it in C under the same leak checkers, with the
RocksDB library found each way a Native application finds it.

## Published artifacts

```sh
XKAFKA_VERSION=0.7.0 scripts/downstream-test.sh
```

Builds a separate application against the published `xkafka-client` artifacts
of the version `XKAFKA_VERSION` names, and runs it against a broker the script
starts, including both ways a Native application reaches librdkafka.

## Formatting

```sh
sbt scalafmtAll scalafmtSbt
```

Verify without mutating using `scalafmtCheckAll` and `scalafmtSbtCheck`.

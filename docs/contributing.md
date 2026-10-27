# Contributing

## Running the tests

```sh
sbt test
```

That runs the ordinary JVM, Scala.js, and Scala Native suites. Tests which need
a broker are skipped unless `XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS` names one,
which the script below sets.

## Broker-backed tests

```sh
scripts/integration-test.sh
```

The script starts one pinned Kafka broker with plaintext, TLS, and SASL_SSL
listeners, runs the same suites against all three backends, then removes the
broker and its volume. Name a backend, `jvm`, `js` or `native`, to run only that
one, as CI does with each on a runner of its own:

```sh
scripts/integration-test.sh native
```

The streams engine's tests run separately, against a broker of each Kafka
release it supports and beside a Java Kafka Streams application, as CI does in
a job of their own:

```sh
scripts/integration-test.sh --streams
```

Three suites need it:

- `KafkaIntegrationSuite` covers produce, consume, and commit round trips.
- `KafkaConformanceSuite` asserts behaviour every backend is expected to share,
  identically on each one.
- `KafkaSecuritySuite` covers TLS and SASL.

A case listed as divergent in the conformance suite is marked as expected to
fail on the named backends. Fixing the backend makes the marked case fail, so a
marker cannot outlive the divergence it records.

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

## The C shim

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

## Published artifacts

```sh
scripts/downstream-test.sh
```

Builds a separate application against the published artifacts and runs it
against a broker, including both ways a Native application reaches librdkafka.

## Formatting

```sh
sbt scalafmtAll scalafmtSbt
```

Verify without mutating using `scalafmtCheckAll` and `scalafmtSbtCheck`.

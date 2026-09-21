# xkafka

xkafka is a functional Kafka client for Scala 3, Cats Effect, and FS2. It
provides the same producer and consumer API on the JVM, Scala.js, and Scala
Native while using the established Kafka client for each runtime.

| Platform | Backend | Runtime |
| --- | --- | --- |
| JVM | [fs2-kafka](https://github.com/typelevel/fs2-kafka) | Java 17 or newer |
| Scala.js | [Confluent Kafka JavaScript](https://github.com/confluentinc/confluent-kafka-javascript) | Node.js 24 |
| Scala Native | [librdkafka](https://github.com/confluentinc/librdkafka) | Scala Native 0.5 or newer |

The API uses `Resource` for producer and consumer lifecycles, `Stream` for
consumer records, effectful serializers and deserializers, and explicit offset
commits. Scala.js targets Node.js rather than browsers.

## Usage

Construct the client the same way on every platform:

```scala
KafkaClient[IO]
```

The selected JVM, Scala.js, or Scala Native artifact supplies the corresponding
backend automatically. Producer and consumer settings use the same portable
model on every platform. For example, a producer can be created and used as
follows:

```scala
import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import cats.syntax.all.*

import xkafka.*

object ProduceExample extends IOApp.Simple:
  private val topic =
    Topic
      .from("events")
      .fold(error => throw new IllegalArgumentException(error.toString), identity)

  private val utf8 = Serializer.utf8[IO]

  private val settings =
    ClientSettings.from(NonEmptyList.one("localhost:9092")).andThen: client =>
      ProducerSettings.from(client, utf8, utf8)

  override def run: IO[Unit] =
    settings.fold(
      errors => IO.raiseError(new IllegalArgumentException(errors.toList.map(_.message).mkString("; "))),
      settings => KafkaClient[IO].producer(settings).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value")))).void
    )
```

`produce` returns `F[F[ProducerResult]]`. The outer effect completes once the
backend has accepted the records for delivery, and the inner one once the
broker has acknowledged them, so several batches can be in flight at once:

```scala
for
  first  <- producer.produce(firstBatch)
  second <- producer.produce(secondBatch)
  _      <- first
  _      <- second
yield ()
```

`produceAndAwait` combines both stages when that pipelining is not wanted.

A `ProducerResult` pairs every record with the metadata its backend reported
for it. A `None` means the backend acknowledged the record but reported no
metadata for it.

Consumers expose an FS2 stream of `CommittableConsumerRecord`. A successful
`record.offset.commit` stores `record.offset.nextOffset`; processing and commit
policy therefore remain explicit in the calling effect. A consumer can
subscribe either to a non-empty list of topics with `Subscription.Topics` or to
topics matching a non-empty `TopicPattern` with `Subscription.Pattern`. Patterns
match complete topic names; portable patterns should use regular-expression
syntax shared by Java, ECMAScript, and POSIX extended regular expressions.

Offsets can be accumulated with `CommittableOffsetBatch.fromFoldable` and
committed together while the consumer resource remains active. A batch retains
only the greatest next offset for each topic-partition and performs one backend
commit per originating consumer. For streaming workloads,
`commitBatchWithin[IO](100, 5.seconds)` is an FS2 pipe which commits whenever it
collects 100 offsets or five seconds elapse, whichever happens first.

Within the consumer resource, `assignment` reports the currently assigned
topic-partitions, `committed` returns their broker-stored next offsets without
sentinel values, `beginningOffsets` and `endOffsets` query the available offset
range, `offsetsForTimes` finds the earliest available offsets at or after given
timestamps, `partitionsFor` and `listTopics` expose visible topic metadata, and
`seek` changes the next offset fetched for an assigned topic-partition.
`assignmentChanges` is an FS2 stream which emits the current assignment and
then each distinct one afterwards. `partitionedRecords` exposes a record
stream for each assigned topic-partition and ends that stream after the
partition is revoked.

The JVM and Scala.js backends report rebalances as the group makes them.
Scala Native looks for one at `ConsumerSettings.pollInterval`, because
librdkafka runs its rebalance callback only while the consumer is polled.

On the JVM the partition streams come from fs2-kafka and are fed
independently. On Scala.js and Scala Native they share one record source, so
consume them concurrently: an unconsumed stream eventually backpressures the
others, and `maxQueuedRecords` bounds how much each buffers first.

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance. Serializers,
deserializers, producer and consumer settings, committable offsets and records,
partition record streams, and consumers have cats-tagless `FunctorK` instances
for transforming their effect with a natural transformation. `KafkaProducer` has
`mapK` but no `FunctorK` instance: translating the acknowledgement nested inside
the enqueue needs a `Functor` for the target effect.
`KafkaClient.imapK` transforms a complete client between effects in both
directions while preserving `Resource` cancellation semantics.

`Serializer.bytes`, `Deserializer.bytes`, `Serializer.utf8`, and
`Deserializer.utf8` provide portable codecs for common values. Their `.option`
combinator represents Kafka null keys and values explicitly as `None`, which is
also how tombstone records should be modeled.

Client, producer, and consumer settings each accept an immutable `properties`
map for backend configuration not modeled directly by xkafka:

```scala
val producer = ClientSettings.from(
  bootstrapServers = NonEmptyList.one("localhost:9092"),
  properties = Map("metadata.max.age.ms" -> "30000")
).andThen: client =>
  ProducerSettings.from(client, utf8, utf8, properties = Map("linger.ms" -> "5"))
```

Producer or consumer properties override client properties. Property names and
values are interpreted by the selected backend; portable applications should
use only properties supported with the same meaning by each target backend.

The `from` constructors return `ValidatedNel[SettingsError, *]`, accumulate
portable configuration errors, and only construct valid settings. They reject
blank property names and bootstrap servers that are not `host:port`. They also
reject the names listed in `ManagedProperties`: bootstrap servers, client and
group IDs, offset reset, automatic commits, and the TLS and SASL names. xkafka derives those from the
typed settings, so supplying them through the map is an error. Backend-specific
configuration remains the selected backend's responsibility.

Each settings type also has `with*` methods for deriving one value from
another. Those that can invalidate the result, such as `withProperty`, return
`ValidatedNel[SettingsError, *]` and revalidate in full.

Backend-reported failures are exposed as `KafkaException.BackendFailure`, which
preserves the original cause and carries a portable `ErrorCode` along with
retriable and fatal classifications. The named `ErrorCode` cases are Kafka
protocol errors, which every broker reports under the same code, plus the
client-side conditions each backend raises on its own; `ErrorCode.Other` carries
a code with no portable meaning. Matching on `ErrorCode.OffsetOutOfRange`
therefore behaves the same on all three platforms.
`KafkaException.InvalidBackendResponse` indicates that a backend returned data
which cannot be represented by the portable API.

## Transport security

TLS and SASL are part of `ClientSettings`, so the same code selects them on
every backend:

```scala
val tls  = TlsSettings.from(CertificateAuthority.PemFile("/etc/kafka/ca.pem"))
val sasl = SaslSettings.from(SaslMechanism.ScramSha256, "user", password)

val client = (tls, sasl).mapN(SecuritySettings.SaslTls.apply).andThen: security =>
  ClientSettings.from(NonEmptyList.one("broker:9093"), security = security)
```

`SecuritySettings` has one case per protocol: `Plaintext`, `Tls`,
`SaslPlaintext`, and `SaslTls`. Each case carries what its protocol needs, so a
client cannot ask for SASL without supplying credentials.

A `CertificateAuthority` is `SystemDefault` for the authorities the runtime
already trusts, `PemFile` for a path, or `Pem` for PEM text held in memory.
Scala.js and Scala Native also accept a directory of certificates as a
`PemFile`. `TlsSettings` verifies the broker hostname unless
`withHostnameVerification(false)` says otherwise. `SaslSettings` accepts
`SaslMechanism.Plain`, `ScramSha256`, and `ScramSha512`, and keeps its password
out of `toString`.

xkafka derives the backend configuration from these, so `security.protocol`,
the `sasl.*` names, and the `ssl.ca.*` and `ssl.truststore.*` names join the
other entries in `ManagedProperties` and are rejected in the `properties` maps.

A rejected broker certificate or an incorrect password fails the call as
`KafkaException.BackendFailure`.

## Building

The build uses sbt. Run all ordinary tests with:

```sh
sbt test
```

The Scala.js backend is installed through sbt-scalajs-bundler. Use the Node.js
version recorded in `.nvmrc`:

```sh
nvm use
```

The Native build downloads librdkafka 2.15.1, verifies its SHA-256 checksum,
builds it, and links it statically. It is built with TLS, SASL SCRAM and
OAUTHBEARER, and gzip and zstd compression enabled, so the Native backend can
reach authenticated brokers and read compressed topics.

librdkafka links these against system libraries, so the build and any
application linking the Native backend need OpenSSL, zlib, and zstd available: `libssl-dev`, `zlib1g-dev`, and `libzstd-dev` on Debian and
Ubuntu, or `brew install openssl@3 zstd zlib` on macOS, where their keg-only
prefixes are discovered automatically. To prepare librdkafka explicitly:

```sh
sbt clientNative/prepareLibrdkafka
```

A prebuilt static installation can be supplied instead:

```sh
XKAFKA_LIBRDKAFKA_PREFIX=/path/to/librdkafka-prefix sbt test
```

The prefix must contain `include/librdkafka/rdkafka.h` and
`lib/librdkafka.a`.

Published Native client artifacts contain the C glue code and request
`-lrdkafka` when the Native backend is reachable. Applications with librdkafka
installed in standard compiler and linker locations need no additional xkafka
settings. For a nonstandard installation, add its include and library
directories to the application's Scala Native configuration:

```scala
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ :+ "-L/path/to/librdkafka/lib")
```

## Integration tests

Docker is required for the broker-backed suite:

```sh
scripts/integration-test.sh
```

The script starts one pinned Kafka broker, runs the same produce, consume, and
commit round trip on JVM, Scala.js, and Scala Native, then removes the broker
and its volume. These tests are skipped by ordinary `sbt test` runs unless
`XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS` is set.

## Modules

- `xkafka-kernel` contains the portable data model and producer and consumer
  algebras.
- `xkafka-client` contains `KafkaClient` and the JVM, Scala.js, and Scala Native
  backends.

## License

xkafka is released under the [MIT License](LICENSE). Third-party components
retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

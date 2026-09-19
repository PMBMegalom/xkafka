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
import java.nio.charset.StandardCharsets

import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.Chunk

import xkafka.*

object ProduceExample extends IOApp.Simple:
  private val topic =
    Topic
      .from("events")
      .fold(error => throw new IllegalArgumentException(error.toString), identity)

  private val utf8 = Serializer.instance[IO, String] { (_, _, value) =>
    IO.pure(
      Some(Chunk.array(value.getBytes(StandardCharsets.UTF_8)))
    )
  }

  private val settings = ProducerSettings(
    client = ClientSettings(NonEmptyList.one("localhost:9092")),
    keySerializer = utf8,
    valueSerializer = utf8
  )

  override def run: IO[Unit] =
    KafkaClient[IO]
      .producer(settings)
      .use(
        _.produce(
          NonEmptyList.one(ProducerRecord(topic, "key", "value"))
        )
      )
      .void
```

Consumers expose an FS2 stream of `CommittableConsumerRecord`. A successful
`record.offset.commit` stores `record.offset.nextOffset`; processing and commit
policy therefore remain explicit in the calling effect. A consumer can
subscribe either to a non-empty list of topics with `Subscription.Topics` or to
topics matching a non-empty `TopicPattern` with `Subscription.Pattern`. Patterns
match complete topic names; portable patterns should use regular-expression
syntax shared by Java, ECMAScript, and POSIX extended regular expressions.

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance. Serializers,
deserializers, producer and consumer settings, committable offsets and records,
producers, and consumers have cats-tagless `FunctorK` instances for transforming
their effect with a natural transformation. `KafkaClient.imapK` transforms a
complete client between effects in both directions while preserving `Resource`
cancellation semantics.

Client, producer, and consumer settings each accept an immutable `properties`
map for backend configuration not modeled directly by xkafka:

```scala
val client = ClientSettings(
  bootstrapServers = NonEmptyList.one("localhost:9092"),
  properties = Map("metadata.max.age.ms" -> "30000")
)

val producer = ProducerSettings(
  client = client,
  keySerializer = utf8,
  valueSerializer = utf8,
  properties = Map("linger.ms" -> "5")
)
```

Producer or consumer properties override client properties. Values managed by
xkafka, including bootstrap servers, client and group IDs, offset reset, and
automatic commits, cannot be overridden through the map. Property names and
values are interpreted by the selected backend; portable applications should
use only properties supported with the same meaning by each target backend.

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
builds it, and links it statically. To prepare it explicitly:

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

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
model everywhere. For example:

```scala
import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}

import xkafka.*

object ProduceExample extends IOApp.Simple:
  private val utf8 = Serializer.utf8[IO]

  private val settings =
    ClientSettings.from(NonEmptyList.one("localhost:9092")).andThen: client =>
      ProducerSettings.from(client, utf8, utf8)

  override def run: IO[Unit] =
    for
      topic    <- Topic.from("events").liftTo[IO]
      producer <- settings.liftTo[IO]
      _        <- KafkaClient[IO].producer(producer).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))
    yield ()
```

## Documentation

The full documentation lives at
[pmbmegalom.github.io/xkafka](https://pmbmegalom.github.io/xkafka/), covering
settings and validation, producing and consuming, offsets and commits,
transport security, the error model, serialization, and the subtle differences
between the three runtimes.

## Building

The build uses sbt. Run all ordinary tests with:

```sh
sbt test
```

Broker-backed tests need Docker:

```sh
scripts/integration-test.sh
```

See [Contributing](https://pmbmegalom.github.io/xkafka/contributing.html).

## Modules

- `xkafka-kernel` contains the portable data model and producer and consumer
  algebras.
- `xkafka-client` contains `KafkaClient` and the JVM, Scala.js, and Scala Native
  backends.

## License

xkafka is released under the [MIT License](LICENSE). Third-party components
retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

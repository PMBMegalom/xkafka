# xkafka

xkafka is a functional Kafka client for Scala 3, Cats Effect, and FS2. It
provides the same producer and consumer API on the JVM, Scala.js, and Scala
Native while using the established Kafka client for each runtime.

| Platform | Kafka client | Runtime |
| --- | --- | --- |
| JVM | [Apache Kafka Java client](https://kafka.apache.org/documentation/#api) | Java 17 or newer |
| Scala.js | [librdkafka](https://github.com/confluentinc/librdkafka) | Node.js 24 |
| Scala Native | [librdkafka](https://github.com/confluentinc/librdkafka) | Scala Native 0.5 or newer |

The API uses `Resource` for producer and consumer lifecycles, `Stream` for
consumer records, effectful serializers and deserializers, and explicit offset
commits. Scala.js targets Node.js rather than browsers.

## Usage

Construct the client the same way on every platform:

```scala
// The same line gives you the client for whichever platform you build for
KafkaClient[IO]
```

Add `xkafka-client` for your platform, and `KafkaClient[IO]` gives you that
platform's client. Settings are the same on every platform. On Scala.js a
prebuilt librdkafka comes from npm, and on Scala Native your application links
an installed librdkafka; see the
[platform notes](https://pmbmegalom.github.io/xkafka/platform-notes.html) for
what each platform needs. For example:

```scala
// Write one record to the topic "events", and wait until it is delivered
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
      producer <- settings.liftTo[IO]  // Raise invalid settings as a failure
      // Acquire a producer, send the record, and release the producer
      _        <- KafkaClient[IO].producer(producer).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))
    yield ()
```

## Streams

xkafka also runs Kafka Streams programs, written with its own DSL. An
application can run some instances on xkafka and others on Java Kafka Streams,
and processes each record at least once or exactly once. It needs brokers of
Kafka 4.2 or newer:

```scala
// Count the records of each key, and write every new count to a topic
val text = Serde.utf8[IO]
val words =
  KStream(Named("read"), input, text, text)
    .groupByKey(text, text)
    .count(Named("count"), Store("counts"))            // Keep each key's count in a store
    .toStream(Named("changes"))                        // Turn each change to a count into a record
    .to(Named("write"), output, text, Serde.long[IO])  // Write each change to the output topic

Topology.of(words).liftTo[IO].flatMap: topology =>
  KafkaClient[IO].streams(settings, topology).use(_.run)  // Join the application and keep processing
```

See [Streams](https://pmbmegalom.github.io/xkafka/streams/index.html) for programs,
guarantees, state and standby tasks.

## Documentation

The full documentation lives at
[pmbmegalom.github.io/xkafka](https://pmbmegalom.github.io/xkafka/), covering
settings and validation, producing and consuming, offsets and commits,
transactions, streams, topic administration, transport security, the error
model, headers, serialization, and the subtle differences between the three runtimes.

## Versioning

xkafka follows early semantic versioning. Upgrading within a series keeps binary
compatibility, so code built against `0.1.0` runs against `0.1.1`. Upgrading
across a series, from `0.1.x` to `0.2.0`, may break compatibility in both binary
and source form. While the major version is `0`, expect the API to change between
series.

## Building

To build xkafka or run its tests, see
[Contributing](https://pmbmegalom.github.io/xkafka/contributing.html).

## Modules

- `xkafka-kernel` contains the data types, the producer, consumer, and
  administration interfaces, and the streams DSL, and connects to nothing itself.
- `xkafka-client` contains `KafkaClient`, which connects to Kafka on the JVM,
  Scala.js, and Scala Native.

## License

xkafka is released under the [MIT License](LICENSE). Third-party components
retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

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
commits. On Scala.js it runs on Node.js, not in browsers.

## Usage

Add `xkafka-client` to your build, where `latestVersion` is the
[latest release](https://github.com/PMBMegalom/xkafka/releases/latest). The
`%%%` operator picks the artifact for the platform you build for:

```scala
// Add xkafka to your build
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % latestVersion
```

`KafkaClient[IO]` then gives you the client for that platform, and settings
are the same on every platform. On Scala.js a prebuilt librdkafka comes from
npm, and on Scala Native your application links an installed librdkafka; see
[Getting started](https://pmbmegalom.github.io/xkafka/getting-started.html)
for what each platform needs. For example:

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
import xkafka.streams.*

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

## Schema registry

xkafka writes and reads Avro, JSON Schema and Protobuf with schemas kept in a Confluent
Schema Registry, on every platform, in Confluent's wire format, so Confluent's
Java serializers read what it writes. Derive a codec for your type and make a
serde from it:

```scala
// Write orders as Avro, registering their schema under "orders-value" on the first write
final case class Order(id: String, placedAt: Instant, note: Option[String] = None) derives AvroCodec

AvroSerde.of[IO, Order](registry, Part.Value).flatMap: serde =>
  ProducerSettings.from(client, Serializer.utf8[IO], serde.serializer).liftTo[IO]
```

See [Schema registry](https://pmbmegalom.github.io/xkafka/schema-registry.html) for
connecting to the registry, derived schemas, schema evolution and subjects.

## Documentation

The full documentation lives at
[pmbmegalom.github.io/xkafka](https://pmbmegalom.github.io/xkafka/), covering
settings and validation, producing and consuming, offsets and commits,
transactions, streams, topic administration, transport security, the error
model, headers, serialization, the schema registry with Avro, JSON Schema and
Protobuf, and what differs between the three platforms.

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

- `xkafka-kernel` contains the data types, serializers, deserializers and
  serdes, the producer, consumer, and administration interfaces, and the
  streams DSL, and connects to nothing itself.
- `xkafka-client` contains `KafkaClient`, which connects to Kafka on the JVM,
  Scala.js, and Scala Native.
- `xkafka-schema-registry` reaches a Confluent Schema Registry, and
  `xkafka-schema-registry-http4s` reaches it through an http4s client.
- `xkafka-avro`, `xkafka-json-schema` and `xkafka-protobuf` write and read Avro,
  JSON Schema and Protobuf with the registry's schemas, with codecs derived from
  Scala types.
- `xkafka-streams-http4s` lets the members of a streams application query each
  other's stores over http4s.
- `xkafka-streams-java`, on the JVM, lets streams programs use code written for
  Java Kafka Streams.

## License

xkafka is released under the [MIT License](LICENSE). Third-party components
retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

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

## Adding xkafka to your build

Add `xkafka-client` to your build, where `latestVersion` is the
[latest release](https://github.com/PMBMegalom/xkafka/releases/latest). The
`%%%` operator picks the artifact for the platform you build for:

```scala
// Add xkafka to your build
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % latestVersion
```

`KafkaClient[IO]` gives you the client for your platform, with the same
settings everywhere. Scala.js and Scala Native need a little setup; see
[Getting started](https://pmbmegalom.github.io/xkafka/getting-started.html).
This program writes one record:

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

xkafka also runs Kafka Streams programs, written with its own DSL. One
application can run some instances on xkafka and others on Java Kafka Streams.
It processes each record at least once or exactly once, as you choose. Streams
need Kafka brokers 4.2 or newer:

```scala
// Count the records of each key, and write every new count to a topic
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val text = Serde.utf8[IO]

val program =
  for
    input    <- Topic.from("words").liftTo[IO]
    output   <- Topic.from("word-counts").liftTo[IO]
    words     = KStream(Named("read"), input, text, text)
                  .groupByKey(text, text)
                  .count(Named("count"), Store("counts"))            // Keep each key's count in a store
                  .toStream(Named("changes"))                        // Turn each change to a count into a record
                  .to(Named("write"), output, text, Serde.long[IO])  // Write each change to the output topic
    topology <- Topology.of(words).liftTo[IO]
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- ApplicationId.from("word-counter").liftTo[IO]
    settings <- StreamsSettings.from(client, id).liftTo[IO]
    _        <- KafkaClient[IO].streams(settings, topology).use(_.run)  // Join the application and keep processing
  yield ()
```

See [Streams](https://pmbmegalom.github.io/xkafka/streams/index.html) for programs,
guarantees, state and standby tasks.

## Schema registry

xkafka writes and reads Avro, JSON Schema and Protobuf with schemas kept in a
Confluent Schema Registry, on every platform. Records use Confluent's format, so
Java applications using Confluent's serializers can read them. Add the module of
your format, and reach the registry through an http4s client:

```scala
// Add Avro serdes, and the http4s client that reaches the registry
libraryDependencies ++= Seq(
  "io.github.pmbmegalom" %%% "xkafka-avro"                   % latestVersion,
  "io.github.pmbmegalom" %%% "xkafka-schema-registry-http4s" % latestVersion,
  "org.http4s"           %%% "http4s-ember-client"           % "0.23.38"
)
```

Derive a codec for your type, and make a serde from it:

```scala
// Write orders as Avro, registering their schema under "orders-value" on the first write
import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import org.http4s.ember.client.EmberClientBuilder

import xkafka.*
import xkafka.avro.{AvroCodec, AvroSerde}
import xkafka.schemaregistry.{Part, RegistrySettings, SchemaRegistry}
import xkafka.schemaregistry.http4s.Http4sTransport

final case class Order(id: String, placedAt: Instant, note: Option[String] = None) derives AvroCodec

val orders: Resource[IO, KafkaProducer[IO, String, Order]] =
  for
    http     <- EmberClientBuilder.default[IO].build
    address  <- Resource.eval(RegistrySettings.from(NonEmptyList.one("http://localhost:8081")).liftTo[IO])
    registry <- Resource.eval(SchemaRegistry.of(address, Http4sTransport(http)))
    serde    <- Resource.eval(AvroSerde.of[IO, Order](registry, Part.Value))
    client   <- Resource.eval(ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO])
    settings <- Resource.eval(ProducerSettings.from(client, Serializer.utf8[IO], serde.serializer).liftTo[IO])
    producer <- KafkaClient[IO].producer(settings)
  yield producer
```

See [Schema registry](https://pmbmegalom.github.io/xkafka/schema-registry.html) for
connecting to the registry, derived schemas, schema evolution and subjects.

## Documentation

The full documentation lives at
[pmbmegalom.github.io/xkafka](https://pmbmegalom.github.io/xkafka/), covering
settings and validation, producing and consuming, offsets and commits,
transactions, streams, topic administration, transport security, the error
model, headers, serialization, the schema registry with Avro, JSON Schema and
Protobuf, generating classes from schema files, and what differs between the
three platforms.

[Getting started](https://pmbmegalom.github.io/xkafka/getting-started.html)
lists the other modules, such as those for ScalaPB, smithy4s, vulcan and
avro4s, and describes how xkafka is versioned.

## Building

To build xkafka or run its tests, see
[Contributing](https://pmbmegalom.github.io/xkafka/contributing.html).

## License

xkafka is released under the [MIT License](LICENSE). Third-party components
retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

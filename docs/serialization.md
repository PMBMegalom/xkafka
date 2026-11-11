# Serialization

A `Serializer[F, A]` writes values of `A` as a record's key or value, and a
`Deserializer[F, A]` reads them back. Both are effectful, and receive the
record's topic and headers alongside the value, so a codec can depend on
either. A `Serde[F, A]` holds one of each, for code that both writes and reads
a type, such as a [streams](streams/README.md) program. All three are in the
`xkafka` package.

## Provided codecs

| Codec | Writes |
| --- | --- |
| `Serializer.bytes`, `Deserializer.bytes`, `Serde.bytes` | the bytes as they are |
| `Serializer.utf8`, `Deserializer.utf8`, `Serde.utf8` | text as UTF-8, as Java's `Serdes.String()` does |
| `Serde.long` | a `Long` as eight big-endian bytes, as Java's `Serdes.Long()` does |

`Serde.long` fails to read a value that is not eight bytes long.

## Writing your own

Build a codec from one that exists with `contramap` and `map`:

```scala mdoc:compile-only
// Write a temperature as its text, and read it back
import cats.effect.IO

import xkafka.*

final case class Celsius(degrees: Double)

val write = Serializer.utf8[IO].contramap[Celsius](_.degrees.toString)
val read  = Deserializer.utf8[IO].map(text => Celsius(text.toDouble))
val serde = Serde(write, read)
```

To work with the bytes, the topic or the headers directly, use
`Serializer.instance` and `Deserializer.instance`, which take a function of the
topic, the headers, and the value or its bytes. A serializer answers
`Option[Chunk[Byte]]`, where `None` writes a record without a value. A
deserializer is given `None` for a record without a value.

## Nulls and tombstones

A record without a value is a tombstone. The provided deserializers fail on one
with `Deserializer.NullValueException`. The `.option` combinator, on a serializer, a
deserializer or a serde, represents a Kafka null key or value explicitly as
`None`:

```scala mdoc:compile-only
// Codecs that read and write a Kafka null as None
import cats.effect.IO

import xkafka.*

val key   = Serializer.utf8[IO].option
val value = Deserializer.utf8[IO].option
```

## Windowed keys

Streams programs that write windowed results to a topic use
`Windowed.timeSerde` and `Windowed.sessionSerde`, in `xkafka.streams`. See
[Writing windowed keys](streams/windows.md#writing-windowed-keys).

## Codecs written for Java (JVM only)

On the JVM, a serializer, deserializer or serde written for Kafka's Java
client can be turned into xkafka's, and used wherever xkafka takes one:

```scala mdoc:compile-only
// Make xkafka codecs from Java's: one for a producer, one for a consumer, and one for a streams program
import cats.effect.IO
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}

import xkafka.*
import xkafka.interop.*                                                 // Adds fromJava, on the JVM only

val write = Serializer.fromJava[IO](new StringSerializer)               // Writes values as Java's StringSerializer does
val read  = Deserializer.fromJava[IO](new StringDeserializer).option    // Reads them back, with None for a record without a value
val serde = Serde.fromJava[IO](Serdes.String())                         // Both at once, as a streams program takes them
```

- The Java codec is given each record's topic and headers, as it is in Java.
- Configure it before you pass it in, and close it once nothing uses it: xkafka calls
  neither `configure` nor `close`.
- Headers a Java serializer adds are not written: it is given a copy of the record's
  headers. Configure a serializer that can write headers of its own not to.
- A record without a value fails the deserializer, as it does xkafka's own;
  read with `.option` to get `None` for it. If the Java deserializer returns `null`
  for a value, the record fails with `JavaNullValueException`.
- Whatever the Java codec throws fails the effect it runs in.

## Schema registry

Avro, JSON Schema and Protobuf values with schemas kept in a Confluent Schema Registry
have serdes of their own. See [Schema registry](schema-registry.md).

## Headers

A record's headers have codecs of their own. See [Headers](headers.md).

## Instances

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance, so codecs compose with
`contramap` and `map`.

`Serializer`, `Deserializer` and `Serde` also have cats-tagless `FunctorK`
instances for transforming their effect with a natural transformation.

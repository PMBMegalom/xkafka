# Serialization

`Serializer[F, A]` and `Deserializer[F, A]` are effectful, and receive the topic
and headers alongside the value, so a codec can depend on either.

## Provided codecs

`Serializer.bytes`, `Deserializer.bytes`, `Serializer.utf8`, and
`Deserializer.utf8` cover the common cases.

## Nulls and tombstones

The `.option` combinator represents Kafka null keys and values explicitly as
`None`:

```scala mdoc:compile-only
// Codecs that read and write a Kafka null as None
import cats.effect.IO

import xkafka.*

val key   = Serializer.utf8[IO].option
val value = Deserializer.utf8[IO].option
```

A tombstone is a record whose value is `None`.

## Codecs written for Java (JVM only)

On the JVM, a serializer, deserializer or streams serde written for Kafka's Java
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
- A record without a value fails a plain deserializer, as it does for xkafka's own;
  read with `.option` to get `None` for it. If the Java deserializer returns `null`
  for a value, the record fails with `JavaNullValueException`.
- Whatever the Java codec throws fails the effect it runs in.

## Headers

A record's headers have codecs of their own. See [Headers](headers.md).

## Instances

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance, so codecs compose with
`contramap` and `map`.

Both also have cats-tagless `FunctorK` instances for transforming their effect
with a natural transformation.

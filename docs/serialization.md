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
import cats.effect.IO

import xkafka.*

val key   = Serializer.utf8[IO].option
val value = Deserializer.utf8[IO].option
```

A tombstone is a record whose value is `None`.

## Header values

A header value is bytes. `HeaderSerializer` and `HeaderDeserializer` encode and
decode them, so a call site does not have to name a charset or a byte order:

```scala mdoc:compile-only
import xkafka.*

val headers = Headers.empty.append("trace", "abc").append("attempt", 2)

val trace = headers.values.head.as[String]
```

| type | encoding |
| --- | --- |
| `String` | UTF-8 |
| `Int` | four bytes, most significant first |
| `Long` | eight bytes, most significant first |
| `Chunk[Byte]` | the bytes themselves |

Reading returns `Either[ValidationError, A]`. A header with no value fails as
`MissingHeaderValue`, and a numeric value of the wrong width fails as
`InvalidHeaderLength` rather than being truncated.

Kafka permits a header to have no value, represented as `Header(name, None)`.
The JVM and Native backends preserve it. Confluent Kafka JavaScript accepts only
strings and buffers in its producer binding, so the Scala.js backend raises
`KafkaException.Unsupported` instead of silently changing a missing value into
empty bytes. `Header(name, Some(Chunk.empty))` remains a distinct, supported
empty value on every backend.

`contramap` and `map` adapt a codec to another type, and `emap` rejects values
the bytes can carry but the type cannot.

Header codecs are plain functions, not effects. A header value is already in
memory by the time one is applied.

## Instances

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance, so codecs compose with
`contramap` and `map`.

Both also have cats-tagless `FunctorK` instances for transforming their effect
with a natural transformation.

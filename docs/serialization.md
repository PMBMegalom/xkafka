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

A tombstone is a record whose value is `None`, so modelling it is the same as
modelling any absent value.

## Instances

With `F` fixed, `Serializer[F, A]` has a Cats `Contravariant` instance and
`Deserializer[F, A]` has a Cats `Functor` instance, so codecs compose with
`contramap` and `map`.

Both also have cats-tagless `FunctorK` instances for transforming their effect
with a natural transformation.

# Settings

Three settings types describe a client. `ClientSettings` covers the connection,
and the producer and consumer settings build on it.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val settings =
  ClientSettings.from(NonEmptyList.one("localhost:9092")).andThen: client =>
    ProducerSettings.from(client, utf8, utf8)
```

## Validation

Every `from` constructor returns `ValidatedNel[SettingsError, *]`. It
accumulates, so one call reports every problem at once, and only valid settings
can be constructed.

`liftTo` carries the failure into the effect:

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val program =
  for
    client <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic  <- Topic.from("events").liftTo[IO]
  yield (client, topic)
```

A rejected value raises `KafkaException.InvalidValue`, which carries the
`ValidationError` that rejected it. Rejected settings raise
`KafkaException.InvalidSettings`, which carries every `SettingsError`, so the
accumulation survives the lift.

Bootstrap servers must be `host:port`, including bracketed IPv6 literals.
Property names must not be blank.

## Properties

Each settings type accepts an immutable `properties` map for backend
configuration the portable model does not describe:

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val settings =
  ClientSettings.from(
    bootstrapServers = NonEmptyList.one("localhost:9092"),
    properties = Map("metadata.max.age.ms" -> "30000")
  ).andThen: client =>
    ProducerSettings.from(client, utf8, utf8, properties = Map("linger.ms" -> "5"))
```

Producer or consumer properties override client properties. Names and values are
interpreted by the selected backend, so portable applications should use only
properties supported with the same meaning by each one.

## Managed properties

`ManagedProperties` lists the names xkafka derives from the typed settings:
bootstrap servers, client and group IDs, offset reset, automatic commits, the
default API timeout, and the TLS and SASL names. Supplying one through the map
is a `SettingsError`, so the typed settings and the map cannot disagree.

## Withers

Each type has `with*` methods for deriving one value from another. Those that
can invalidate the result, such as `withProperty`, return
`ValidatedNel[SettingsError, *]` and revalidate in full.

## Timeouts

`ConsumerSettings` carries two, and both mean the same thing on every backend.

| setting | default | what it bounds |
| --- | --- | --- |
| `pollTimeout` | 100ms | how long one poll waits for records before returning empty |
| `requestTimeout` | 60s | how long a call that asks the broker something waits for its answer |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, and `seek`. Its default
matches what Kafka's own clients use.

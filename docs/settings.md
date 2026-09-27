# Settings

Three settings types describe a client. `ClientSettings` covers the connection,
and the producer and consumer settings build on it. `TransactionalProducerSettings`
wraps the producer settings, and [Transactions](transactions.md) covers what it adds.

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
    properties = Map("client.rack" -> "eu-west-1a")
  ).andThen: client =>
    ProducerSettings.from(client, utf8, utf8, properties = Map("linger.ms" -> "5"))
```

Producer or consumer properties override client properties. Names and values are
interpreted by the selected backend, so portable applications should use only
properties supported with the same meaning by each one.

## Managed properties

`ManagedProperties` lists the names xkafka derives from the typed settings:
bootstrap servers, client and group IDs, offset reset, automatic commits, the
default API timeout, the metadata refresh names, the isolation level, the
transaction names, and the TLS and SASL names.
Supplying one through the map is a `SettingsError`, so the typed settings and the
map cannot disagree.

## Withers

Each type has `with*` methods for deriving one value from another. Those that
can invalidate the result, such as `withProperty`, return
`ValidatedNel[SettingsError, *]` and revalidate in full.

## Commit recovery

`ConsumerSettings.commitRecovery` says how a failed offset commit is retried. It
defaults to `CommitRecovery.Default`. See [Offsets](offsets.md).

## Isolation

`ConsumerSettings` carries `isolationLevel`, which says whether records of a
transaction that has not committed are delivered. It defaults to
`ReadUncommitted`, which is Kafka's own default. See
[Transactions](transactions.md).

## Timeouts

`ConsumerSettings` carries two, and both mean the same thing on every backend.

| setting | default | what it bounds |
| --- | --- | --- |
| `pollTimeout` | 100ms | how long one poll waits for records before returning empty |
| `requestTimeout` | 60s | how long a call that asks the broker something waits for its answer |
| `commitTimeout` | 15s | how long a commit waits before it fails as a timed out request |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, and `seek`. Its default
matches what Kafka's own clients use.

`ProducerSettings` carries one, because only a producer has work of its own to
finish on the way out.

| setting | default | what it bounds |
| --- | --- | --- |
| `closeTimeout` | 60s | how long releasing a producer waits to deliver what it holds |

`ClientSettings` carries one more, because it applies to producers and consumers
alike.

| setting | default | what it bounds |
| --- | --- | --- |
| `metadataRefreshInterval` | 5m | how long a new topic, partition, or leader can go unnoticed |

Each backend spells it differently, so the typed setting is what keeps the two
the same: the Java client takes it as `metadata.max.age.ms`, and librdkafka takes
it as `topic.metadata.refresh.interval.ms` and derives its own cache age from it.

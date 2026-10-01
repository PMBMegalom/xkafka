# Settings

`ClientSettings` describes the connection. `ProducerSettings` and
`ConsumerSettings` each build on it and add what their client needs.
`TransactionalProducerSettings` wraps `ProducerSettings`, and is covered in
[Transactions](transactions.md).

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
Property names must not be blank. Every duration must be positive and at most
`Int.MaxValue` milliseconds (about 24.9 days), which is the largest timeout the
JavaScript and Native backend calls can represent without overflow.

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
acknowledgement and transaction names, and the TLS and SASL names.
Supplying one through the map is a `SettingsError`, so the typed settings and the
map cannot disagree.

## Withers

Each type has `with*` methods for deriving one value from another. Those that
can invalidate the result, such as `withProperty`, return
`ValidatedNel[SettingsError, *]` and revalidate in full. This includes every
duration wither: `withMetadataRefreshInterval`, `withCloseTimeout`,
`withTransactionTimeout`, `withCommitTimeout`, `withPollTimeout`, and
`withRequestTimeout`.

## Consumer settings

Beyond the group and the deserializers, `ConsumerSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `autoOffsetReset` | `Latest` | where to start when the group has no committed offset |
| `isolationLevel` | `ReadUncommitted` | whether records from uncommitted transactions are delivered |
| `commitRecovery` | `CommitRecovery.Default` | how a failed commit is retried |
| `pollTimeout` | 100ms | how long one poll waits for records before returning empty |
| `requestTimeout` | 60s | how long a call that asks the broker something waits for an answer |
| `commitTimeout` | 15s | how long a commit waits before failing as a timed out request |
| `assignmentFencing` | `true` | whether an offset commits only while this consumer still holds the partition it was read from |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, and `seek`. Its default matches
what Kafka's own clients use.

See [Offsets](offsets.md) for commit recovery, the commit timeout, and assignment
fencing, and [Transactions](transactions.md) for the isolation level.

## Producer settings

Beyond the serializers, `ProducerSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `acks` | `Acks.AllReplicas` | how many replicas must hold a record before it is acknowledged |
| `closeTimeout` | 60s | maximum flush wait for records already accepted by a producer |
| `idempotence` | `true` | whether the producer is idempotent, Kafka's `enable.idempotence`, which needs `acks` to be `AllReplicas` |

See [Producing](producing.md) for both.

## Client settings

`ClientSettings` applies to producers and consumers alike:

| setting | default | meaning |
| --- | --- | --- |
| `metadataRefreshInterval` | 5m | how long a new topic, partition, or leader can go unnoticed |

Setting this once covers every platform. Kafka spells it
`metadata.max.age.ms` and librdkafka spells it
`topic.metadata.refresh.interval.ms`, so look for those names when comparing
against their documentation.

`ClientSettings` also carries the bootstrap servers, an optional client id, and
the security settings. See [Transport security](transport-security.md).

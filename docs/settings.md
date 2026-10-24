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
`Int.MaxValue` milliseconds (about 24.9 days). Some have a lower limit:
`linger` may be zero and at most `ProducerSettings.LingerLimit` (15 minutes),
and the group membership intervals are limited by
`ConsumerSettings.SessionTimeoutLimit` (1 hour),
`ConsumerSettings.HeartbeatIntervalLimit` (1 hour), and
`ConsumerSettings.MaxPollIntervalLimit` (24 hours).

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
    ProducerSettings.from(client, utf8, utf8, properties = Map("retry.backoff.ms" -> "200"))
```

Producer or consumer properties override client properties. Names and values are
interpreted by the selected backend, so portable applications should use only
properties supported with the same meaning by each one.

## Managed properties

`ManagedProperties` is the set of property names xkafka sets from the typed
settings, such as `bootstrap.servers`, `group.id`, `acks`, `linger.ms`, and the
TLS and SASL names. Supplying one of them through the map is a `SettingsError`;
use the typed setting instead.

## Withers

Each type has `with*` methods for deriving one value from another. Those that
can invalidate the result, such as `withProperty`, return
`ValidatedNel[SettingsError, *]` and revalidate in full. This includes every
duration wither: `withMetadataRefreshInterval`, `withCloseTimeout`,
`withTransactionTimeout`, `withCommitTimeout`, `withPollTimeout`,
`withRequestTimeout`, `withLinger`, `withEnqueueTimeout`, `withSessionTimeout`,
`withHeartbeatInterval`, `withMaxPollInterval`, `withRebalanceSafeCommits`, and
`withStartingOffsets`.

## Consumer settings

Beyond the group and the deserializers, `ConsumerSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `autoOffsetReset` | `Latest` | where to start when the group has no committed offset, or one no longer in the partition |
| `isolationLevel` | `ReadUncommitted` | whether records from uncommitted transactions are delivered |
| `commitRecovery` | `CommitRecovery.Default` | how a failed commit is retried |
| `pollTimeout` | 100ms | how long one poll waits for records before returning empty |
| `requestTimeout` | 60s | how long a call that asks the broker something waits for an answer |
| `commitTimeout` | 15s | how long a commit waits before failing as a timed out request |
| `assignmentFencing` | `true` | whether offsets from a partition this consumer no longer owns are refused |
| `groupInstanceId` | none | an identity that makes the consumer a static member of its group |
| `sessionTimeout` | unset, which is 45s on every platform | how long the group waits to hear from the consumer before handing its partitions on |
| `heartbeatInterval` | unset, which is 3s on every platform | how often the consumer tells the group it is alive |
| `maxPollInterval` | 5m | how long the consumer can go without fetching before it leaves its group |
| `rebalanceSafeCommits` | none | how long a partition being taken away waits for the records handed out from it to be committed |
| `startingOffsets` | none | where each assigned partition starts, from offsets the application keeps |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, `seek`, `seekToBeginning`, and
`seekToEnd`. Its default matches
what Kafka's own clients use.

See [Offsets](offsets.md) for commit recovery, the commit timeout, assignment
fencing, rebalance-safe commits, and starting offsets, [Transactions](transactions.md) for the isolation level, and
[Group membership](consuming.md#group-membership) for `groupInstanceId` and the
three intervals.

## Producer settings

Beyond the serializers, `ProducerSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `acks` | `Acks.AllReplicas` | how many replicas must hold a record before it is acknowledged |
| `closeTimeout` | 60s | maximum flush wait for records already accepted by a producer |
| `idempotence` | `true` | whether Kafka's `enable.idempotence` is on, which requires `Acks.AllReplicas` |
| `compression` | `Compression.Uncompressed` | how record batches are compressed |
| `linger` | 5ms | how long a record waits for others to join its batch, from zero to 15 minutes |
| `batchSize` | unset, which leaves it to the platform's client | the most bytes one batch holds before it is sent |
| `enqueueTimeout` | 60s | how long `produce` waits for room for a record while the client's buffer is full |

See [Producing](producing.md) for each of them.

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

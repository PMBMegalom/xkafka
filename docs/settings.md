# Settings

`ClientSettings` describes the connection. `ProducerSettings` and
`ConsumerSettings` each build on it and add what their client needs.
`TransactionalProducerSettings` wraps `ProducerSettings`, and is covered in
[Transactions](transactions.md). `StreamsSettings`, in `xkafka.streams`, also
builds on `ClientSettings`, and is [described below](#streams-settings).

```scala mdoc:compile-only
// Validate the connection, then build producer settings on it
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val settings =
  ClientSettings.from(NonEmptyList.one("localhost:9092")).andThen: client =>  // Runs only if the connection settings are valid
    ProducerSettings.from(client, utf8, utf8)
```

## Validation

Every settings `from` constructor returns `ValidatedNel[SettingsError, *]`. It
accumulates, so one call reports every problem at once, and only valid settings
can be constructed. `StreamsSettings` works the same way with
`StreamsSettingsError`, and its `liftTo` raises `InvalidStreamsSettings`.

`liftTo` carries the failure into the effect:

```scala mdoc:compile-only
// Turn invalid values or settings into a failure of the effect
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val program =
  for
    client <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]  // Fails with InvalidSettings
    topic  <- Topic.from("events").liftTo[IO]                                     // Fails with InvalidValue
  yield (client, topic)
```

A rejected value raises `KafkaException.InvalidValue`, which carries the
`ValidationError` that rejected it. Rejected settings raise
`KafkaException.InvalidSettings`, which carries every `SettingsError`, so the
accumulation survives the lift.

Bootstrap servers must be `host:port`, including bracketed IPv6 literals.
Property names must not be blank. Every duration, in these settings and in
`StreamsSettings`, must be a whole number of milliseconds, since Kafka's clients
count no finer. It must also be positive. In these settings it must be at most
`Int.MaxValue` milliseconds (about 24.9 days); `StreamsSettings` sets no such
limit. The exceptions are:

- `linger` may be zero, and must be less than `ProducerSettings.LingerLimit`
  (5 minutes). A transactional producer's linger must also be less than its
  `transactionTimeout`.
- `transactionTimeout` is at least
  `TransactionalProducerSettings.TransactionTimeoutMinimum` (1 second).
- `metadataRefreshInterval` is at most
  `ClientSettings.MetadataRefreshIntervalLimit` (1 hour).
- `fetchWait` may be zero, and at most `ConsumerSettings.FetchWaitLimit` (5 minutes).
- The streams `taskTimeout` may be zero.
- The group membership intervals are at most
  `ConsumerSettings.SessionTimeoutLimit` (1 hour),
  `ConsumerSettings.HeartbeatIntervalLimit` (1 hour), and
  `ConsumerSettings.MaxPollIntervalLimit` (24 hours).

## Properties

Client, producer, consumer and transactional producer settings each accept a
`properties` map for Kafka client properties that have no typed setting:

```scala mdoc:compile-only
// Pass Kafka properties that have no typed setting
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val settings =
  ClientSettings.from(
    bootstrapServers = NonEmptyList.one("localhost:9092"),
    properties = Map("client.rack" -> "eu-west-1a")  // For everything built on this client
  ).andThen: client =>
    // For this producer only, and overrides the client's
    ProducerSettings.from(client, utf8, utf8, properties = Map("retry.backoff.ms" -> "200"))
```

Producer or consumer properties override client properties. Properties go
straight to the platform's client: the Java client on the JVM, and librdkafka on
Scala.js and Scala Native. The two accept different names, so for code that runs
on the JVM and elsewhere, use only properties both accept with the same meaning.

## Managed properties

`ManagedProperties` is the set of property names xkafka sets from the typed
settings, such as `bootstrap.servers`, `group.id`, `acks`, `linger.ms`, and the
TLS and SASL names, with the other names librdkafka reads them under, such as
`metadata.broker.list`. Supplying one of them through the map fails as
`SettingsError.ManagedProperty`; use the typed setting instead.

## Changing settings

Each type has `with*` and `without*` methods for deriving one value from
another. Those that can make the settings invalid return
`ValidatedNel[SettingsError, *]` and validate the result in full: every one
that sets a duration or properties, and `withBootstrapServers`, `withAcks`,
`withIdempotence`, `withBatchSize`, `withGroupInstanceId`,
`withRebalanceSafeCommits`, `withStartingOffsets`, `withoutSessionTimeout`, and
`withoutHeartbeatInterval`. `StreamsSettings` works the same way: its methods
that set a duration, the concurrency or the state directory return
`ValidatedNel[StreamsSettingsError, *]`. The others, such as
`withCompression` or `withIsolationLevel`, return the settings directly.

```scala mdoc:compile-only
// Change two settings, each of which validates the result again
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def tuned[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  settings.withFetchWait(100.millis).andThen(_.withProperty("fetch.min.bytes", "65536")).liftTo[IO]
```

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
| `groupInstanceId` | none | an identity that makes the consumer a static member of its group, named as a topic is |
| `sessionTimeout` | unset, which is 45s on every platform | how long the group waits to hear from the consumer before handing its partitions on |
| `heartbeatInterval` | unset, which is 3s on every platform | how often the consumer tells the group it is alive |
| `maxPollInterval` | 5m | how long the consumer can go without fetching before it leaves its group |
| `fetchWait` | 500ms | how long the broker may hold a fetch waiting for records, from zero up to 5m |
| `rebalanceSafeCommits` | none | how long a partition being taken away waits for the records handed out from it to be committed |
| `startingOffsets` | none | where each assigned partition starts, from offsets the application keeps |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, `seek`, `seekToBeginning`, and
`seekToEnd`.

`fetchWait` is the longest the broker holds a fetch while fewer than
`fetch.min.bytes` of records are waiting. `fetch.min.bytes` is an ordinary
property, 1 byte by default. With that default the broker answers as soon as any
record arrives, and `fetchWait` only sets how often a consumer with nothing to
read asks again. Raise `fetch.min.bytes` to have the broker gather records into
fewer, larger answers, and `fetchWait` then bounds how long a record can wait to
be gathered. The Java client calls this setting `fetch.max.wait.ms` and
librdkafka `fetch.wait.max.ms`. Both are [managed properties](#managed-properties),
so set it through `withFetchWait`.

See [Offsets](offsets.md) for commit recovery, the commit timeout, assignment
fencing, rebalance-safe commits, and starting offsets, [Transactions](transactions.md) for the isolation level, and
[Group membership](consuming.md#group-membership) for `groupInstanceId` and the
three intervals.

## Producer settings

Beyond the serializers, `ProducerSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `acks` | `Acks.AllReplicas` | how many replicas must hold a record before it is acknowledged |
| `closeTimeout` | 60s | how long releasing the producer waits for records it has not delivered yet |
| `idempotence` | `true` | whether Kafka's `enable.idempotence` is on, which requires `Acks.AllReplicas` |
| `compression` | `Compression.Uncompressed` | how record batches are compressed |
| `linger` | 5ms | how long a record waits for others to join its batch, from zero to under 5 minutes |
| `batchSize` | unset, which leaves it to the platform's client | the most bytes one batch holds before it is sent |
| `enqueueTimeout` | 60s | how long `produce` waits for room for a record while the client's buffer is full |

See [Producing](producing.md) for each of them.

## Streams settings

Beyond the client and the application's id, `StreamsSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `guarantee` | `ProcessingGuarantee.AtLeastOnce` | whether a record can be processed again after work moves |
| `commitInterval` | unset, which is 30s with at-least-once and 100ms with exactly-once processing | how often tasks commit |
| `concurrency` | 1 | how many tasks process records at once |
| `closeTimeout` | 30s | how long releasing the application waits to give back its tasks and leave |
| `topologyDescriptionPush` | `true` | whether a member sends the program's description when the broker asks for it |
| `queryEndpoint` | unset, so other members cannot query this one | where this member answers other members' [queries](streams/queries.md) |
| `stateDirectory` | unset, so stores are kept in memory | where the application keeps its stores on disk ([Keeping stores on disk](streams/running.md#keeping-stores-on-disk)) |
| `fetchWait` | 500ms | how long the broker may hold a fetch of the application's consumers, from zero up to 5m |
| `taskTimeout` | 5m | how long the application keeps asking the group's coordinator to read or commit offsets while it is not ready, does not know the member yet, or while asking fails in a way worth retrying, before it stops with the last failure; with zero, it stops at the first |

See [Streams](streams/running.md#guarantees) for each of them. `fetchWait` works
as a [consumer's](#consumer-settings) does, and `fetch.min.bytes`, set in the
client settings' properties, reaches the application's consumers too.

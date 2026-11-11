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
Property names must not be blank. Every duration must be positive and at most
`Int.MaxValue` milliseconds (about 24.9 days), with these exceptions:

- `linger` may be zero, and at most `ProducerSettings.LingerLimit` (15 minutes).
- `fetchWait` may be zero, and at most `ConsumerSettings.FetchWaitLimit` (5 minutes).
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

Producer or consumer properties override client properties. Each platform's client reads
the names and values itself: the Java client on the JVM, and librdkafka on Scala.js
and Scala Native. For code that runs on more than one platform, use only properties
that both accept with the same meaning.

## Managed properties

`ManagedProperties` is the set of property names xkafka sets from the typed
settings, such as `bootstrap.servers`, `group.id`, `acks`, `linger.ms`, and the
TLS and SASL names. Supplying one of them through the map fails as
`SettingsError.ManagedProperty`; use the typed setting instead.

## Changing settings

Each type has `with*` and `without*` methods for deriving one value from
another. Those that can make the settings invalid return
`ValidatedNel[SettingsError, *]` and validate the result in full: every one
that sets a duration or properties, and `withBootstrapServers`, `withAcks`,
`withIdempotence`, `withBatchSize`, `withGroupInstanceId`,
`withRebalanceSafeCommits`, and `withStartingOffsets`. The others, such as
`withCompression` or `withIsolationLevel`, return the settings directly.

```scala mdoc:compile-only
// Change two settings, each of which validates the result again
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def tuned[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  settings.withFetchWait(100.millis).andThen(_.withProperty("fetch.min.bytes", "65536")).liftTo[IO]
```

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
| `fetchWait` | 500ms | how long the broker may hold a fetch waiting for records, from zero up to 5m |
| `rebalanceSafeCommits` | none | how long a partition being taken away waits for the records handed out from it to be committed |
| `startingOffsets` | none | where each assigned partition starts, from offsets the application keeps |

`requestTimeout` bounds `committed`, `beginningOffsets`, `endOffsets`,
`offsetsForTimes`, `partitionsFor`, `listTopics`, `seek`, `seekToBeginning`, and
`seekToEnd`. Its default matches
what Kafka's own clients use.

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
| `closeTimeout` | 60s | maximum flush wait for records already accepted by a producer |
| `idempotence` | `true` | whether Kafka's `enable.idempotence` is on, which requires `Acks.AllReplicas` |
| `compression` | `Compression.Uncompressed` | how record batches are compressed |
| `linger` | 5ms | how long a record waits for others to join its batch, from zero to 15 minutes |
| `batchSize` | unset, which leaves it to the platform's client | the most bytes one batch holds before it is sent |
| `enqueueTimeout` | 60s | how long `produce` waits for room for a record while the client's buffer is full |

See [Producing](producing.md) for each of them.

## Streams settings

Beyond the client and the application's id, `StreamsSettings` carries:

| setting | default | meaning |
| --- | --- | --- |
| `guarantee` | `ProcessingGuarantee.AtLeastOnce` | whether a record can be processed again after work moves |
| `commitInterval` | unset, which is 30s at least once and 100ms exactly once | how often tasks commit |
| `concurrency` | 1 | how many tasks process records at once |
| `closeTimeout` | 30s | how long releasing the application waits to give back its tasks and leave |
| `topologyDescriptionPush` | `true` | whether a member sends the program's description when the broker asks for it |
| `queryEndpoint` | unset, so other members cannot query this one | where this member answers other members' [queries](streams/queries.md) |
| `stateDirectory` | unset, so stores are kept in memory | where the application keeps its stores on disk ([Keeping stores on disk](streams/running.md#keeping-stores-on-disk)) |

See [Streams](streams/running.md#guarantees) for each of them.

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

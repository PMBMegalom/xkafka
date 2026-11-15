# Consuming

A consumer is a `Resource` and exposes an FS2 stream of
`CommittableConsumerRecord`.

```scala mdoc:compile-only
// Read ten records from "events" as a member of the group "workers"
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val utf8 = Deserializer.utf8[IO]

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group    <- ConsumerGroup.from("workers").liftTo[IO]
    // Start at the first record if the group has no committed offset
    settings <- ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Earliest).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    records  <-
      KafkaClient[IO].consumer(settings, Selection.Topics(NonEmptySet.one(topic)))  // Read the partitions of this topic that the group assigns to this consumer
        .use(_.records.take(10).compile.toList)                                     // Stop after ten records, then release the consumer
  yield records
```

The example reads records without committing them. Nothing is committed until
your code asks; [Offsets](offsets.md) shows how and when to commit.

## What a record carries

Besides its topic-partition, offset, key, value and [headers](headers.md), a `ConsumerRecord`
has:

| field | meaning |
| --- | --- |
| `timestamp` | `RecordTimestamp.CreateTime` if the producer or its client set the time, `LogAppendTime` if the broker set it because the topic's `message.timestamp.type` is `LogAppendTime`, or `None` |
| `leaderEpoch` | the leader epoch of the partition when the record was written, or `None` if the client did not report it |
| `serializedKeySize` | the key's size in bytes as it was read, or `None` for a record without a key |
| `serializedValueSize` | the value's size in bytes as it was read, or `None` for a record without a value |

## Choosing topics or partitions

`Selection` says where a consumer's records come from.

`Selection.Topics` takes a non-empty set of topics. `Selection.Pattern` takes a
`TopicPattern`, which matches complete topic names. The JVM reads it as a Java
regular expression, and Scala.js and Scala Native as one of librdkafka's, which
differ. For code that runs on every platform, keep to simple patterns such as
`events-.*`.
Both join a consumer group, so the partitions a consumer holds follow the
group's rebalances.

@:callout(warning)
A consumer that subscribes to a topic before that topic exists sees it once the
consumer refreshes its metadata. `metadataRefreshInterval` sets how long that
takes and defaults to five minutes. Lower it if a consumer must discover a topic
promptly.
@:@

`Selection.Partitions` takes a non-empty set of topic-partitions and reads
exactly those. It joins no group, so the assignment never changes. Two consumers
that name the same partition both read all of it, even in the same consumer
group.

Committed offsets are still stored under the consumer group. Consumers reading
the same partitions under one group therefore share those offsets, and one
resuming from a commit continues from where the other left off.

@:callout(warning)
Kafka stores the last offset committed for a partition without comparing it to
what is already there. Two consumers committing the same partition race, and the
later commit wins even when its offset is lower, which moves the group backwards
and replays records. A batch keeps the highest offset per partition, but only
among the offsets in that batch. Give consumers their own group unless they are
meant to share a position.
@:@

By default a consumer delivers records from transactions that have not
committed. Set `isolationLevel` to change that. See
[Transactions](transactions.md).

## Where a consumer starts

A consumer resumes each partition from its group's committed offset. If the
group has none, `autoOffsetReset` decides:

| value | where the consumer starts |
| --- | --- |
| `Earliest` | at the first record still in the partition |
| `Latest` | at the end of the partition, reading only records written from then on |
| `Fail` | nowhere: the record stream fails as `ErrorCode.OffsetResetRequired` |

The same setting applies if the committed offset is no longer in the partition,
for example because its records were deleted. `Earliest` starts at the first
record still there, and `Fail` fails the record stream.

Choose `Fail` if skipping or replaying records would be wrong, for example when
you rebuild state from a snapshot plus the records written after it.

## Processing records

`records` is one stream of the records from every partition the consumer is
assigned, each paired with the offset that commits it. You handle each record
and commit when you choose, as [Offsets](offsets.md) describes.

`consumeChunk` does both for you. It takes a function over a chunk of records
and commits that chunk once the function returns:

```scala
// Handle each chunk of records, then commit it
consumer.consumeChunk: records =>
  records.traverse_(handle).as(CommitNow)  // CommitNow is what commits the chunk
```

Partitions are processed concurrently, so a slow chunk holds back only the
partition it came from.

`consumeChunk` runs until it is cancelled, until something fails, or until
`stopConsuming` has been called and everything already fetched has been
processed. A chunk whose commit fails with `ErrorCode.IllegalGeneration`,
because the group moved its partition while it was being processed, is left
uncommitted, and `consumeChunk` goes on: the member that has the partition now
reads those records again.

## A stream per partition

`partitionedRecords` exposes a record stream for each assigned topic-partition
and ends that stream after the partition is revoked.

```scala
// Give each assigned partition its own stream of records, and handle them
consumer.partitionedRecords(maxQueuedRecords = 256).map { partition =>  // Pause a partition once 256 of its records are waiting
  partition.records.evalMap(handle)
}.parJoinUnbounded  // Consume every partition's stream at once
```

Read every partition's stream at the same time, for example with
`parJoinUnbounded`. The consumer pauses a partition once `maxQueuedRecords` of
its records are waiting. Records already being fetched can briefly take it past
that limit.

## Stopping without losing records

`stopConsuming` stops fetching. The record streams then end once the records
already fetched have been handed over. Nothing read from the broker is dropped,
and offsets remain committable.

Run it alongside the stream:

```scala
// Consume until a shutdown is requested, then finish what was already fetched
val consume = consumer.records.evalMap(record => handle(record) *> record.offset.commit).compile.drain  // Handle and commit each record
val stop    = shutdownRequested.get *> consumer.stopConsuming  // Stop fetching when a shutdown is requested

(consume, stop).parTupled  // Run both side by side
```

`stopConsuming` returns as soon as fetching has been told to stop, without
waiting for the streams to drain. Calling it more than once has no further
effect, and a stream started after it is empty.

Interrupting the stream instead, with `interruptWhen` for example, stops it
mid-record and discards whatever had been fetched. Releasing the consumer
resource, or cancelling whatever reads it, does the same. The consumer that next
reads those partitions gets the uncommitted records again.

## Group membership

Four settings control how a consumer stays in its group. They are checked
together when you build the settings, and a combination any platform would
refuse is a `SettingsError`:

- `sessionTimeout` is how long the group waits to hear from the consumer before
  handing its partitions to the others. The broker also bounds it, through its
  `group.min.session.timeout.ms` and `group.max.session.timeout.ms`, which
  default to six seconds and thirty minutes.
- `heartbeatInterval` is how often the consumer tells the group it is alive. It
  must be less than the session timeout.
- `maxPollInterval` is how long the consumer can go without fetching records.
  It stops fetching while too many records wait unread, so if your code reads no
  records for longer than this, the consumer leaves its group. It must be at
  least the session timeout, unless `group.protocol` is `consumer`.
- `groupInstanceId` makes the consumer a static member of its group. It follows
  the rules for topic names: at most 249 letters, digits, `.`, `_` or `-`, and
  not `.` or `..`.

Unless they are set, the session timeout and heartbeat interval are 45 seconds
and 3 seconds on every platform. If you set the property `group.protocol` to
`consumer`, the broker decides both. Leave them unset then: a consumer with
either one set fails as `ErrorCode.InvalidConfiguration`.

```scala mdoc:compile-only
// Make a consumer a static member, with an identity of its own and a longer session timeout
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def staticMember[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  // Each of these calls validates the settings again
  settings.withGroupInstanceId("orders-1").andThen(_.withSessionTimeout(2.minutes)).andThen(_.withMaxPollInterval(5.minutes)).liftTo[IO]
```

Each of these methods validates the settings again as a whole, so raising the
session timeout above the maximum poll interval fails unless the poll interval
is raised first.

@:callout(warning)
A consumer joins its group as soon as it is made, so a consumer nobody reads
still takes a share of the partitions. Release consumers you are not reading.
@:@

### Static members

A dynamic member leaves its group when it is released, and the others take its
partitions at once. A static member does not: the group keeps its partitions
for it until its session times out. One that comes back under the same
`groupInstanceId` within the session timeout gets the same partitions back, and
the rest of the group carries on with theirs. One that stays away loses its
partitions once its session times out.

Give each static member an identity of its own. A longer session timeout gives a
restarting member more time to come back, at the cost of leaving its partitions
unread for that long if it does not.

## Inspecting the consumer

Within the consumer resource:

- `assignment` reports the currently assigned topic-partitions.
- `assignmentChanges` emits the current assignment and then each distinct one.
- `committed` returns broker-stored next offsets, with the leader epoch and
  metadata each was committed with, and `None` for a partition with no
  committed offset.
- `beginningOffsets` and `endOffsets` query the available offset range.
- `offsetsForTimes` finds the earliest available offsets at or after timestamps.
  A time before 1970 raises `IllegalArgumentException`, as does writing a record
  with one, since Kafka holds none.
- `partitionsFor` returns a topic's partitions. For a topic the cluster does
  not have, it fails with `ErrorCode.UnknownTopicOrPartition` and creates
  nothing.
- `listTopics` returns every topic the consumer can see, with its partitions.
- `seek` changes the next offset fetched for an assigned topic-partition.
- `seekToBeginning` and `seekToEnd` move to either end of an assigned topic-partition.
- `position` reports the offset a topic-partition reads next.
- `stopConsuming` stops fetching and lets the streams drain.

@:callout(info)
`position` is where the client fetches the partition next. The client reads
ahead of the records the stream has handed out, so after consuming, `position`
is at or past the offset after the last record consumed, by an amount that
differs between platforms. Before the consumer has consumed from the partition,
it can be `None`, and it is `None` for a partition of a known topic that the
consumer is not assigned.
@:@

@:callout(info)
After a seek, the next record the stream hands out of that partition comes from
the offset sought, and no record fetched before the seek is handed out. A seek
of a partition the consumer has not been assigned fails with
`ErrorCode.UnknownTopicOrPartition`. On Scala.js and Scala Native, a seek is
also refused until the partition is being fetched, which happens shortly after
it is assigned. If you seek right after starting a consumer, retry briefly.
@:@

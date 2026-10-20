# Consuming

A consumer is a `Resource` and exposes an FS2 stream of
`CommittableConsumerRecord`.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val utf8 = Deserializer.utf8[IO]

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group    <- ConsumerGroup.from("workers").liftTo[IO]
    settings <- ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Earliest).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    records  <-
      KafkaClient[IO].consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
        .use(_.records.take(10).compile.toList)
  yield records
```

## What a record carries

Besides its topic-partition, offset, key, value and headers, a `ConsumerRecord`
has:

| field | meaning |
| --- | --- |
| `timestamp` | `RecordTimestamp.CreateTime` where the producer set the time, or `LogAppendTime` where the topic's `message.timestamp.type` is `LogAppendTime` and the broker set it |
| `leaderEpoch` | the leader epoch of the partition when the record was written |
| `serializedKeySize` | the key's size in bytes as it was read, or `None` for a record without a key |
| `serializedValueSize` | the value's size in bytes as it was read, or `None` for a record without a value |

## What a consumer reads

`Selection` says where a consumer's records come from.

`Selection.Topics` takes a non-empty set of topics. `Selection.Pattern` takes a
`TopicPattern`, which matches complete topic names. Portable patterns should use
regular-expression syntax shared by Java, ECMAScript, and POSIX extended regular
expressions. Both join a consumer group, so the partitions a consumer holds
follow the group's rebalances.

`Selection.Partitions` takes a non-empty set of topic-partitions and reads
exactly those. It joins no group, so the assignment never changes. Two consumers
naming the same partition each read all of it, even when they share a consumer
group, because no group membership divides the partition between them.

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

@:callout(info)
A seek is refused until the partition it names is being fetched, which happens
after the assignment arrives. Retry briefly if you seek immediately after
starting a consumer.
@:@

By default a consumer delivers records from transactions that have not
committed. Set `isolationLevel` to change that. See
[Transactions](transactions.md).

## Group membership

A consumer that joins a group stays in it while it keeps in touch with the
group. Four settings govern that, and each is checked when the settings are
built, so a combination either backend would refuse is a `SettingsError`:

- `sessionTimeout` is how long the group waits to hear from the consumer before
  handing its partitions to the others. The broker also bounds it, through its
  `group.min.session.timeout.ms` and `group.max.session.timeout.ms`, which
  default to six seconds and thirty minutes.
- `heartbeatInterval` is how often the consumer tells the group it is alive. It
  must be less than the session timeout.
- `maxPollInterval` is how long the consumer can go without fetching records.
  Fetching stops while too many records wait unread, so a consumer whose records
  go unread for longer than this leaves its group. It must be at least the
  session timeout.
- `groupInstanceId` makes the consumer a static member of its group.

Unless they are set, the session timeout and heartbeat interval are left to the
backend, and both backends use the same defaults: 45 seconds and 3 seconds.
Leave them unset when the group uses the newer consumer group protocol, where the
broker decides them.

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def staticMember[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  settings.withGroupInstanceId("orders-1").andThen(_.withSessionTimeout(2.minutes)).andThen(_.withMaxPollInterval(5.minutes)).liftTo[IO]
```

Each wither revalidates the settings as a whole, so raising the session timeout
above the maximum poll interval fails unless the poll interval is raised first.

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

## Where a consumer starts

A consumer resumes each partition from its group's committed offset. Where the
group has none, `autoOffsetReset` decides:

| value | where the consumer starts |
| --- | --- |
| `Earliest` | at the first record still in the partition |
| `Latest` | at the end of the partition, reading only records written from then on |
| `Fail` | nowhere: the record stream fails as `ErrorCode.OffsetResetRequired` |

A committed offset that is no longer in the partition, because the records it
pointed at were deleted, is handled by the same setting. `Earliest` starts at the
first record still there, and `Fail` fails the record stream.

Choose `Fail` where skipping or replaying records would be wrong, such as for a
replica rebuilt from a snapshot and the log written after it.

## Inspecting the consumer

Within the consumer resource:

- `assignment` reports the currently assigned topic-partitions.
- `assignmentChanges` emits the current assignment and then each distinct one.
- `committed` returns broker-stored next offsets, with the leader epoch and
  metadata each was committed with, and `None` for a partition with no
  committed offset.
- `beginningOffsets` and `endOffsets` query the available offset range.
- `offsetsForTimes` finds the earliest available offsets at or after timestamps.
- `partitionsFor` and `listTopics` expose visible topic metadata.
- `seek` changes the next offset fetched for an assigned topic-partition.
- `seekToBeginning` and `seekToEnd` move to either end of an assigned topic-partition.
- `position` reports the offset a topic-partition reads next.
- `stopConsuming` stops fetching and lets the streams drain.

@:callout(info)
`position` returns `None` until this consumer has consumed from the partition.
Some backends settle on a position sooner, for instance when a seek names an
offset, so the value every backend agrees on is the one after records have been
consumed.
@:@

## Processing every record

`consumeChunk` takes a function over a chunk of records and commits that chunk
once the function returns:

```scala
consumer.consumeChunk: records =>
  records.traverse_(handle).as(CommitNow)
```

Partitions are processed concurrently, so a slow chunk holds back only the
partition it came from. Returning `CommitNow` is what triggers the commit.

`consumeChunk` runs until it is cancelled, until something fails, or until
`stopConsuming` has been called and everything already fetched has been
processed.

@:callout(warning)
A consumer joins its group as soon as it is made, so a consumer nobody reads
still takes a share of the partitions. Release consumers you are not reading.
@:@

## Stopping

`stopConsuming` stops fetching. The record streams then end once the records
already fetched have been handed over. Nothing read from the broker is dropped,
and offsets remain committable.

Run it alongside the stream:

```scala
val consume = consumer.records.evalMap(record => handle(record) *> record.offset.commit).compile.drain
val stop    = shutdownRequested.get *> consumer.stopConsuming

(consume, stop).parTupled
```

`stopConsuming` returns as soon as fetching has been told to stop, without
waiting for the streams to drain. Calling it more than once has no further
effect, and a stream started after it is empty.

Interrupting the stream instead, with `interruptWhen` for example, stops it
mid-record and discards whatever had been fetched. Releasing the consumer
resource, or cancelling whatever reads it, does the same. In those cases whoever
resumes the group reads those offsets again.

## Partition streams

`partitionedRecords` exposes a record stream for each assigned topic-partition
and ends that stream after the partition is revoked.

```scala
consumer.partitionedRecords(maxQueuedRecords = 256).map { partition =>
  partition.records.evalMap(handle)
}.parJoinUnbounded
```

Every emitted stream must be consumed concurrently. `maxQueuedRecords` is the
point at which the backend pauses a partition. Records already in flight can
temporarily take its buffer past that threshold.

@:callout(warning)
A consumer that subscribes to a topic before that topic exists sees it once the
backend refreshes its metadata. `metadataRefreshInterval` sets how long that
takes and defaults to five minutes. Lower it if a consumer must discover a topic
promptly.
@:@

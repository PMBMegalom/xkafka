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

## Inspecting the consumer

Within the consumer resource:

- `assignment` reports the currently assigned topic-partitions.
- `assignmentChanges` emits the current assignment and then each distinct one.
- `committed` returns broker-stored next offsets, without sentinel values.
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
Consumers join their group at different moments on different backends. The Java
client joins when the application polls, so a consumer nobody reads holds no
partitions. The JavaScript and Native backends join when the consumer resource is
allocated, so a consumer nobody reads still takes a share of the partitions and
does not give them back. Release consumers you are not reading.
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

Every emitted stream must be consumed concurrently. `maxQueuedRecords` bounds
how far ahead each partition buffers before it is held back.

@:callout(warning)
A consumer that subscribes to a topic before that topic exists sees it once the
backend refreshes its metadata. `metadataRefreshInterval` sets how long that
takes and defaults to five minutes. Lower it if a consumer must discover a topic
promptly.
@:@

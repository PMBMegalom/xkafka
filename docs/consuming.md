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

`Selection.Topics` takes a non-empty set of topics. `Selection.Pattern` takes a non-empty
`TopicPattern`, which matches complete topic names. Portable patterns should use
regular-expression syntax shared by Java, ECMAScript, and POSIX extended regular expressions.
Both join a consumer group, so the partitions a consumer holds follow the group's rebalances.

`Selection.Partitions` takes a non-empty set of topic-partitions and reads exactly those. No group
is joined, so the assignment never changes and nothing rebalances it away. Two consumers naming the
same partition each read all of it, even where they share a consumer group, because there is no
group membership to divide it between them.

The consumer group is still the key that committed offsets are stored under, so consumers naming the
same partitions under one group do share those offsets, and a consumer resuming from a commit reads
from wherever the other left off.

@:callout(warning)
Kafka stores whatever was committed last for a partition, with no comparison against what is already
there. Two consumers committing the same partition therefore race, and the later commit wins even
where its offset is lower, which moves the group backwards and replays records. A batch keeps the
highest offset per partition, but only among the offsets in that batch. Give consumers their own
group unless they are meant to share a position.
@:@

@:callout(info)
A seek is refused until the partition it names is being fetched, which holding the assignment does
not yet mean. Retry briefly if you seek immediately after a consumer starts.
@:@

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

@:callout(info)
`position` answers `None` until this consumer has consumed from the partition. A backend may
settle on a position sooner, such as when a seek names an offset, so the value every backend
agrees on is the one after records have been consumed.
@:@

## Processing every record

`consumeChunk` takes a function over a chunk of records and commits that chunk once the function
returns, so the common case needs no stream plumbing.

```scala
consumer.consumeChunk: records =>
  records.traverse_(handle).as(CommitNow)
```

Partitions are processed alongside one another, so a slow chunk holds back only the partition it
came from. The result never produces a value, because the work ends only by cancellation or failure.
Returning `CommitNow` is what makes the commit visible where the records are handled.

@:callout(warning)
A consumer joins its group at a different moment on each backend. The Java client joins when the
application polls, so a consumer nobody reads holds nothing. The JavaScript and Native backends
join when the consumer resource is allocated, so one nobody reads still takes a share of the
partitions and does not hand them back. Release a consumer you are not reading.
@:@

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
backend refreshes its metadata. `metadataRefreshInterval` sets how long that takes
and defaults to five minutes, so lower it if a consumer must discover a topic
promptly.
@:@

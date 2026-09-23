# Consuming

A consumer is a `Resource` and exposes an FS2 stream of
`CommittableConsumerRecord`.

```scala mdoc:compile-only
import cats.data.NonEmptyList
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
      KafkaClient[IO].consumer(settings, Subscription.Topics(NonEmptyList.one(topic)))
        .use(_.records.take(10).compile.toList)
  yield records
```

## Subscriptions

`Subscription.Topics` takes a non-empty list of topics.
`Subscription.Pattern` takes a non-empty `TopicPattern`, which matches complete
topic names. Portable patterns should use regular-expression syntax shared by
Java, ECMAScript, and POSIX extended regular expressions.

## Inspecting the consumer

Within the consumer resource:

- `assignment` reports the currently assigned topic-partitions.
- `assignmentChanges` emits the current assignment and then each distinct one.
- `committed` returns broker-stored next offsets, without sentinel values.
- `beginningOffsets` and `endOffsets` query the available offset range.
- `offsetsForTimes` finds the earliest available offsets at or after timestamps.
- `partitionsFor` and `listTopics` expose visible topic metadata.
- `seek` changes the next offset fetched for an assigned topic-partition.

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

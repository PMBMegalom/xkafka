# Offsets

Every record arrives as a `CommittableConsumerRecord`, pairing the record with
the offset that commits it. Commits are explicit, so processing and commit
policy stay in the calling effect.

A successful `record.offset.commit` stores `record.offset.nextOffset`, which is
the offset after the one just handled.

## Batches

Offsets can be accumulated and committed together while the consumer resource
is active:

```scala
CommittableOffsetBatch.fromFoldable(offsets).commit
```

A batch retains only the greatest next offset for each topic-partition, and
performs one backend commit per originating consumer.

## Committing on a schedule

`commitBatchWithin` is an FS2 pipe which commits whenever it collects `n`
offsets or `d` elapses, whichever happens first:

```scala
consumer.records
  .evalTap(handle)
  .map(_.offset)
  .through(commitBatchWithin(100, 5.seconds))
```

Only non-empty batches are committed.

## Recovering a failed commit

A commit fails for two kinds of reason. Some say the broker is moving, such as a
coordinator that is loading or a group that is rebalancing, and those clear on
their own. The rest say the commit will never be accepted, and retrying one only
delays the failure.

`ConsumerSettings.commitRecovery` retries the first kind. It defaults to ten
attempts, doubling from 10ms, held at 10s, and spread by a fifth:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Deserializer.utf8[IO]

val settings =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group    <- ConsumerGroup.from("workers").liftTo[IO]
    policy   <- CommitRecovery.exponential(maxAttempts = 5, initialDelay = 50.millis, maxDelay = 2.seconds).liftTo[IO]
    settings <- ConsumerSettings.from(client, group, utf8, utf8).liftTo[IO]
  yield settings.withCommitRecovery(policy)
```

`CommitRecovery.none` turns it off, so a failed commit fails. What counts as
worth retrying is `ErrorCode.retriable`, which is the same on every backend, so
all three retry the same conditions and wait the same way.

The spread is what keeps consumers that hit one condition together from
returning together. It is applied after the doubling stops growing, so a delay
can exceed `maxDelay` by that fraction.

When every attempt is used up, the commit fails with
`KafkaException.CommitFailed`, which carries how many attempts were made, the
offsets that were not committed, and the failure that caused the last one.

@:callout(info)
Recovery wraps the committer, so it applies wherever a commit happens: a single
`CommittableOffset.commit`, a batch, `commitBatchWithin`, and `consumeChunk`.
@:@

## Transformations

`CommittableOffset`, `CommittableOffsetBatch`, and `CommittableConsumerRecord`
all have cats-tagless `FunctorK` instances, so an offset can be moved between
effects with a natural transformation and still commit through the consumer it
came from.

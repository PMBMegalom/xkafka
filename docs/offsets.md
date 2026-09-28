# Offsets

Every record arrives as a `CommittableConsumerRecord`, pairing the record with
the offset that commits it. Commits are explicit, so processing and commit
policy stay in the calling effect.

A successful `record.offset.commit` stores `record.offset.nextOffset`, the
offset after the one just handled.

## Batches

Offsets can be accumulated and committed together while the consumer resource
is active:

```scala
CommittableOffsetBatch.fromFoldable(offsets).commit
```

A batch keeps only the greatest next offset for each topic-partition, and makes
one backend commit per originating consumer.

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

## Retrying a failed commit

Some commit failures clear on their own, such as a coordinator that is loading
or a group that is rebalancing. Others never will, and retrying them only delays
the failure.

`ConsumerSettings.commitRecovery` retries the first kind. `ErrorCode.retriable`
decides which failures qualify, and it answers the same on every backend:

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

The default policy makes ten attempts, starting at 10ms, doubling each time, and
stopping the doubling at 10s. Each delay is then spread by a fifth, so that
consumers which hit one condition together do not all retry at the same moment.
The spread is applied after the cap, so a delay can exceed `maxDelay` by that
fraction.

`CommitRecovery.none` disables retrying. When every attempt has been used, the
commit fails with `KafkaException.CommitFailed`, which carries the number of
attempts, the offsets that were not committed, and the failure from the last
attempt.

@:callout(info)
Recovery wraps the committer, so it applies wherever a commit happens: a single
`CommittableOffset.commit`, a batch, `commitBatchWithin`, and `consumeChunk`.
@:@

## How long a commit waits

`ConsumerSettings.commitTimeout` bounds a commit and defaults to fifteen
seconds. A commit that does not finish in time fails as
`ErrorCode.RequestTimedOut`. That code is retriable, so the recovery policy
tries again:

```scala
settings.withCommitTimeout(5.seconds)
```

@:callout(warning)
A commit that timed out may still be applied by the broker afterwards, because
the request has already been sent. Committing the same offset twice has no
further effect, so a later attempt that succeeds leaves the group in the same
place.
@:@

## Transformations

`CommittableOffset`, `CommittableOffsetBatch`, and `CommittableConsumerRecord`
all have cats-tagless `FunctorK` instances, so an offset can be moved between
effects with a natural transformation and still commit through the consumer it
came from.

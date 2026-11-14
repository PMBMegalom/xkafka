# Offsets

Every record arrives as a `CommittableConsumerRecord`, pairing the record with
the offset that commits it. Nothing is committed until your code asks, so you
decide when.

A successful `record.offset.commit` stores `record.offset.nextOffset`, the
offset after the one just handled, so a consumer that resumes from it starts
with the next record:

```scala
// Handle each record, then commit it
consumer.records.evalMap(record => handle(record) *> record.offset.commit)
```

Each commit is a request to the broker, so committing every record on its own is slow.
[Batches](#batches) and [committing on a schedule](#committing-on-a-schedule)
commit less often.

## Batches

Offsets can be accumulated and committed together while the consumer resource
is active:

```scala
// Commit several offsets together, keeping the greatest for each partition
CommittableOffsetBatch.fromFoldable(offsets).commit
```

A batch keeps only the greatest next offset for each topic-partition, and
commits once for each consumer its offsets came from.

## Committing on a schedule

`commitBatchWithin` is an FS2 pipe which commits whenever it collects `n`
offsets or `d` elapses, whichever happens first. Both must be positive; the
stream fails with `IllegalArgumentException` otherwise:

```scala
// Handle each record, and commit every 100 offsets or every 5 seconds, whichever comes first
consumer.records
  .evalTap(handle)
  .map(_.offset)                               // Keep only each record's offset
  .through(commitBatchWithin(100, 5.seconds))  // Collect and commit them in batches
```

Only non-empty batches are committed.

## Retrying a failed commit

Some commit failures clear on their own, such as a coordinator that is loading
or a group that is rebalancing. Others never will, and retrying them only delays
the failure.

`ConsumerSettings.commitRecovery` retries the first kind. `ErrorCode.retriable`
decides which failures qualify, and it is the same on every platform:

```scala mdoc:compile-only
// Retry a failed commit up to five times, waiting longer between attempts
import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Deserializer.utf8[IO]

val settings =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group    <- ConsumerGroup.from("workers").liftTo[IO]
    // Delays start at 50ms, and stop growing at 2 seconds
    policy   <- CommitRecovery.exponential(maxAttempts = 5, initialDelay = 50.millis, maxDelay = 2.seconds).liftTo[IO]
    settings <- ConsumerSettings.from(client, group, utf8, utf8).liftTo[IO]
  yield settings.withCommitRecovery(policy)  // Use the policy for this consumer's commits
```

`maxAttempts` counts the retries after the first failure. The default policy
retries up to ten times, waiting 10ms before the first retry, doubling the wait
each time, and stopping the doubling at 10s. Each delay then varies randomly by
up to 20% either way, so consumers that fail together do not retry together. A
delay can therefore be up to 20% above `maxDelay`.

A failure that is not retriable fails the commit at once, as reported. With a
policy that retries, one that is still retriable after every retry fails it with
`KafkaException.CommitFailed`, which carries the number of attempts, the
offsets that were not committed, and the failure from the last attempt.
`CommitRecovery.none` disables retrying, so every failure fails the commit as
reported, retriable or not.

@:callout(info)
The recovery policy applies wherever a commit happens: a single
`CommittableOffset.commit`, a batch, `commitBatchWithin`, and `consumeChunk`.
@:@

## How long a commit waits

`ConsumerSettings.commitTimeout` bounds a commit and defaults to fifteen
seconds. A commit that does not finish in time fails as
`ErrorCode.RequestTimedOut`. That code is retriable, so the recovery policy
tries again:

```scala mdoc:compile-only
// Let a commit wait at most five seconds before it fails as timed out
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def withCommitTimeout[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  settings.withCommitTimeout(5.seconds).liftTo[IO]
```

Like every method that sets a duration, it returns a `ValidatedNel`; see
[Changing settings](settings.md#changing-settings).

@:callout(warning)
A commit that timed out may still be applied by the broker afterwards, because
the request has already been sent. Committing the same offset twice has no
further effect, so a later attempt that succeeds leaves the group in the same
place.
@:@

## Storing a note with an offset

Each offset is committed with its record's leader epoch, which you don't need to
set. It can also carry text of your own, which the broker stores beside it and
`committed` returns:

```scala
// Commit this record's offset, with a note of your own stored beside it
record.offset.withMetadata("processed by worker 3").commit
```

The broker treats empty metadata as no metadata. It refuses metadata longer than its `offset.metadata.max.bytes`, 4096
bytes by default, and the commit fails with `ErrorCode.OffsetMetadataTooLarge`.
A batch keeps the metadata of the offset it keeps, and a
[transaction](transactions.md) stores both the epoch and the metadata of the
offsets it records.

## Offsets and rebalances

By default, an offset can only be committed while the consumer that read it still
owns its partition. This is called assignment fencing.

When the group moves a partition to another consumer, offsets read from it before
the move can no longer be committed, whether on their own, in a batch, or in a
[transaction](transactions.md). The commit fails with a
`KafkaException.BackendFailure` whose code is `ErrorCode.IllegalGeneration`, and
nothing is committed for that partition. This holds even if the partition later
comes back to the same consumer.

`IllegalGeneration` is not retriable. Read the records again after the rebalance,
and commit those offsets.

How many offsets a rebalance affects depends on the partition assignment strategy:

- With the default strategy, every rebalance revokes all partitions and assigns
  them again, so offsets read before any rebalance are refused, even for the
  partitions the consumer keeps.
- With a cooperative strategy, only the partitions that move are revoked, so
  offsets for the partitions a consumer keeps can still be committed. Choose
  one through the `partition.assignment.strategy` property:
  `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` on the JVM, and
  `cooperative-sticky` on Scala.js and Scala Native.

To commit offsets that did not come from a record, use the consumer's committer,
`record.offset.committer`. Its `commit` takes a `Map[TopicPartition, Offset]` and
commits each with no leader epoch and no metadata; `commitWithMetadata` takes an
`OffsetAndMetadata` for each partition. For these offsets, fencing only checks
that the consumer owns each partition when the commit runs.

To turn fencing off:

```scala
// Let offsets commit even after their partition has moved
settings.withoutAssignmentFencing
```

Without fencing, an offset read before a rebalance can still be committed after its
partition has moved to another consumer, which moves that partition back to an
earlier position.

## Committing before a partition moves

When a partition moves, the records your consumer is still handling from it can
no longer be committed, so the consumer that gets the partition reads them
again. Rebalance-safe commits give those records time to be committed first:

```scala
// Give records up to 30 seconds to be committed before their partition moves
settings.withRebalanceSafeCommits(30.seconds).liftTo[IO]  // Raise invalid settings as a failure
```

Before the consumer gives up a partition, it waits until every record it has
handed out from that partition is committed, for at most the limit. While it
waits:

- committing those records succeeds, one at a time, in a batch, or in a
  [transaction](transactions.md) once the transaction commits;
- the consumer hands out no further records of that partition, and the consumer
  that gets the partition reads them instead;
- the rest of the group waits for the rebalance to finish.

Keep the limit short for that last reason. It must be less than
`maxPollInterval`.

If the limit passes with records still uncommitted, the consumer gives up the
partition anyway, and those records are read again. To stop consuming instead:

```scala
// Wait in the same way, but fail the record stream if records are still uncommitted after 30 seconds
settings.withRebalanceSafeCommits(30.seconds, RebalanceSafeCommits.AtLimit.Fail).liftTo[IO]
```

The record stream then fails with `KafkaException.UncommittedRevocation`, whose
`topicPartitions` names the partitions that still had records uncommitted.

A consumer that has already been removed from its group, for instance because
it went longer than `maxPollInterval` without fetching, gives up its partitions
without waiting. Committing a record it read before it was removed fails with
`ErrorCode.UnknownMemberId`. If it was the group's only member, the broker may
already have removed the empty group, and the commit fails with
`ErrorCode.GroupIdNotFound` instead.

## Starting from offsets you keep

An application that stores how far it has got itself, for example in the same
database transaction as its results, can tell the consumer where each partition
starts:

```scala
// Start each assigned partition from the offset your application stored
settings
  .withStartingOffsets(10.seconds)(loadPositions)  // loadPositions must answer within 10 seconds with an F[Map[TopicPartition, StartingOffset]]
  .liftTo[IO]                                      // Raise invalid settings as a failure
```

The consumer calls the function with the partitions each rebalance assigns,
before it reads any of their records. A consumer reading `Selection.Partitions`
calls it once, when it is created. Each partition the function returns an offset
for starts there, and no earlier record of it is handed out. A `StartingOffset` can also carry the
leader epoch of the last record you handled, `record.offset.leaderEpoch`, if
you stored it.

- A partition the function returns nothing for starts as it would without the
  function: at its committed offset, or where `autoOffsetReset` says if it has
  none.
- An offset past the end of the partition also starts where `autoOffsetReset`
  says.
- The function must return within its limit, which must be less than
  `maxPollInterval`. If it fails or passes the limit, the record stream fails,
  with the function's own failure or with
  `KafkaException.StartingOffsetsTimedOut`. With `Selection.Partitions`,
  creating the consumer fails instead.

## Changing the effect type

An offset can be moved to another effect with a natural transformation and
still commit through the consumer it came from. See
[Changing the effect type](platform-notes.md#changing-the-effect-type).

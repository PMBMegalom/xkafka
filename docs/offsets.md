# Offsets

Every record arrives as a `CommittableConsumerRecord`, pairing the record with
the offset that commits it. Commits are explicit, so processing and commit
policy stay in the calling effect.

A successful `record.offset.commit` stores `record.offset.nextOffset`, the
offset after the one just handled.

## Leader epoch and metadata

An offset commits with the leader epoch of the record it was read with. It can
also carry text of your own, which the broker stores beside it and `committed`
returns:

```scala
record.offset.withMetadata("processed by worker 3").commit
```

The broker keeps no difference between empty metadata and none, so empty means
none. It refuses metadata longer than its `offset.metadata.max.bytes`, 4096
bytes by default, and the commit fails with `ErrorCode.OffsetMetadataTooLarge`.
A batch keeps the metadata of the offset it keeps, and a
[transaction](transactions.md) stores both the epoch and the metadata of the
offsets it records.

## Batches

Offsets can be accumulated and committed together while the consumer resource
is active:

```scala
CommittableOffsetBatch.fromFoldable(offsets).commit
```

A batch keeps only the greatest next offset for each topic-partition, and makes
one backend commit per originating consumer.

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
  offsets for the partitions a consumer keeps can still be committed.

`committer.commit(offsets)` takes offsets that did not come from a record, so it
can only check that the consumer owns each partition at the time of the commit.
It commits them with no leader epoch and no metadata;
`committer.commitWithMetadata` takes an `OffsetAndMetadata` for each partition.

To turn fencing off:

```scala
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
settings.withRebalanceSafeCommits(30.seconds)
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
settings.withRebalanceSafeCommits(30.seconds, RebalanceSafeCommits.AtLimit.Fail)
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
settings.withStartingOffsets(10.seconds): partitions =>
  loadPositions(partitions) // F[Map[TopicPartition, StartingOffset]]
```

The consumer calls the function with the partitions each rebalance assigns,
before it reads any of their records, and once at the start with partitions
named directly. Each partition it answers for starts at the offset given, and
no earlier record of it is handed out. A `StartingOffset` can also carry the
leader epoch of the last record you handled, `record.offset.leaderEpoch`, if
you stored it.

- A partition the function answers nothing for starts as it would without the
  function: at its committed offset, or where `autoOffsetReset` says if it has
  none.
- An offset past the end of the partition also starts where `autoOffsetReset`
  says.
- The function must answer within its limit, which must be less than
  `maxPollInterval`. If it fails or passes the limit, the record stream fails,
  with the function's own failure or with
  `KafkaException.StartingOffsetsTimedOut`. For partitions named directly,
  creating the consumer fails instead.

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

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def withCommitTimeout[F[_], K, V](settings: ConsumerSettings[F, K, V]): IO[ConsumerSettings[F, K, V]] =
  settings.withCommitTimeout(5.seconds).liftTo[IO]
```

Like every duration wither, it revalidates and returns
`ValidatedNel[SettingsError, ConsumerSettings[F, K, V]]`. See
[Settings](settings.md).

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

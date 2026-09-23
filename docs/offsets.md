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

## Transformations

`CommittableOffset`, `CommittableOffsetBatch`, and `CommittableConsumerRecord`
all have cats-tagless `FunctorK` instances, so an offset can be moved between
effects with a natural transformation and still commit through the consumer it
came from.

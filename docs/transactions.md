# Transactions

A transactional producer writes records and records consumer offsets as one unit. Everything it
wrote becomes visible together when the transaction commits, and none of it at all where the
transaction aborts.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- TransactionalId.from("order-writer").liftTo[IO]
    settings <- TransactionalProducerSettings.from(client, id, utf8, utf8).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    record    = ProducerRecord(topic, "key", "value")
    _        <- KafkaClient[IO].transactionalProducer(settings).use(_.transactionally(_.produce(NonEmptyList.one(record))))
  yield ()
```

`transactionally` commits where its body succeeds and aborts where the body fails or is cancelled,
so the transaction never outlives the work it covers. A producer carries one transactional id, and
Kafka allows that id one transaction at a time, so concurrent callers wait for one another.

@:callout(warning)
That waiting is what makes a nested transaction deadlock. A `transactionally` inside the body of
another one on the same producer waits for a transaction that cannot finish until the body returns.
Use one transaction per unit of work, and a producer of its own where two must overlap.
@:@

A transactional producer writes only inside transactions, which is why `transactionalProducer` is a
constructor of its own rather than a mode of `producer`.

## Reading what a transaction wrote

Records of an open transaction are already in the log. A consumer reads them unless it is told not
to, so a reader that should see only committed work asks for it:

```scala
settings.withIsolationLevel(IsolationLevel.ReadCommitted)
```

The default is `ReadUncommitted`, which is Kafka's own, and which delivers every record as soon as
it is written, including records of a transaction that later aborts.

## Read, process, write

`commitOffsets` records the offsets a consumer reached as part of the transaction, so the records
produced and the position they were produced from land together or not at all.

```scala
consumer.records.chunks.evalMap: chunk =>
  val batch = CommittableOffsetBatch.fromFoldable(chunk.map(_.offset))
  producer.transactionally: transaction =>
    transaction.produce(derive(chunk)) *> transaction.commitOffsets(batch)
```

The batch carries the committer each offset came from, and that committer knows its consumer, so
naming the wrong group is not something this can express.

@:callout(warning)
A transactional pipeline is only atomic end to end when its reader also uses
`IsolationLevel.ReadCommitted`. Otherwise the downstream consumer sees records of a transaction that
has not committed, and may see records an abort later withdrew.
@:@

@:callout(info)
`produce` inside a transaction waits for the broker to acknowledge the records, because a
transaction cannot commit records that have not been acknowledged. It therefore returns
`F[ProducerResult]`, without the second stage a plain producer offers.
@:@

## Settings

`TransactionalProducerSettings` wraps `ProducerSettings` and adds the two values Kafka needs.

| setting | default | what it means |
| --- | --- | --- |
| `transactionalId` | required | Kafka's `transactional.id` |
| `transactionTimeout` | 60s | Kafka's `transaction.timeout.ms`, defaulting to what its own producers use |

Give each producer instance that must run alongside another its own `transactionalId`. Allocating a
second producer under an id already in use fences the first, so its open transaction can no longer
commit. That is what the id is for when a replacement takes over from an instance that has gone, and
a failure when both were meant to run.

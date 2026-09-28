# Transactions

A transactional producer writes records and consumer offsets as one unit.
Everything it wrote becomes visible together when the transaction commits, and
none of it becomes visible if the transaction aborts.

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

`transactionally` commits the transaction if its body succeeds, and aborts it if
the body fails or is cancelled.

A transactional producer only writes inside transactions, which is why
`transactionalProducer` is a separate constructor rather than an option on
`producer`.

Kafka allows one transaction at a time per transactional id, so concurrent calls
to `transactionally` on one producer run one after another.

@:callout(warning)
Do not nest `transactionally` on the same producer. The inner call waits for the
outer transaction to finish, and the outer transaction cannot finish until its
body returns, so the pair deadlocks. Use one transaction per unit of work, or a
second producer with its own transactional id.
@:@

## Reading what a transaction wrote

Records written by an open transaction are already in the log, and a consumer
delivers them by default. To read only committed records, set the isolation
level:

```scala
settings.withIsolationLevel(IsolationLevel.ReadCommitted)
```

The default is `IsolationLevel.ReadUncommitted`, which is also Kafka's default.
It delivers every record as soon as it is written, including records from a
transaction that later aborts.

## Read, process, write

`commitOffsets` adds the offsets a consumer has reached to the transaction, so
the records produced and the position they were produced from are committed
together:

```scala
consumer.records.chunks.evalMap: chunk =>
  val batch = CommittableOffsetBatch.fromFoldable(chunk.map(_.offset))
  producer.transactionally: transaction =>
    transaction.produce(derive(chunk)) *> transaction.commitOffsets(batch)
```

The batch already identifies the consumer its offsets came from, so
`commitOffsets` needs no consumer argument and cannot record them against the
wrong group.

@:callout(warning)
The pipeline is only atomic end to end if whatever reads the output topic also
sets `IsolationLevel.ReadCommitted`. A reader left on the default sees records
from transactions that have not committed, including ones that later abort.
@:@

@:callout(info)
`produce` inside a transaction returns `F[ProducerResult]` rather than the two
stages a plain producer offers. A transaction cannot commit records the broker
has not acknowledged, so the call waits for them.
@:@

## Settings

`TransactionalProducerSettings` wraps `ProducerSettings` and adds two values.

| setting | default | maps to |
| --- | --- | --- |
| `transactionalId` | required | Kafka's `transactional.id` |
| `transactionTimeout` | 60s | Kafka's `transaction.timeout.ms` |

Give each producer that runs alongside another its own `transactionalId`.
Creating a second producer with an id already in use fences the first, and its
open transaction can then no longer commit. That is the intended behaviour when a
replacement takes over from an instance that has stopped, and a source of
failures when two producers were meant to run at the same time.

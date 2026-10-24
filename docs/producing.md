# Producing

A producer is a `Resource`. Records are produced in non-empty batches.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    settings <- ProducerSettings.from(client, utf8, utf8).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    _        <- KafkaClient[IO].producer(settings).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))
  yield ()
```

## Two stages

`produce` returns `F[F[NonEmptyList[Produced]]]`. The outer effect completes
once the backend has accepted the records for delivery, and the inner one once
the backend reports their delivery outcome. With acknowledgements enabled this
includes the broker's response. Holding the inner effect lets several batches be
in flight at once:

```scala
for
  first  <- producer.produce(firstBatch)
  second <- producer.produce(secondBatch)
  _      <- first
  _      <- second
yield ()
```

`produceAndAwait` runs both stages together, for callers that do not need the
pipelining.

For a stream of batches, use `pipe`:

```scala
batches.through(producer.pipe(maxInFlight = 256))
```

`pipe` enqueues later batches while earlier ones still await their delivery
outcome, and emits results in the order the batches arrived. `maxInFlight` sets
how many batches may await delivery at once. Mapping `produceAndAwait` over a
stream instead would send one batch at a time.

To write records and consumer offsets atomically, use a transactional producer.
See [Transactions](transactions.md).

## Results

Each `Produced` holds a record and the metadata its backend reported for it, in
the order the records were given:

```scala
producer.produceAndAwait(batch).map(_.map(produced => produced.metadata.offset))
```

`RecordMetadata` carries the topic-partition, and the offset and timestamp where
Kafka reports them.

If any record is not delivered, the inner effect fails with the failure of the
first such record. If the client refuses a record as it is enqueued, the outer
effect fails instead.

These records are not refused as they are enqueued. They fail the inner effect,
as records that are not delivered do:

- A record larger than the client takes fails with
  `ErrorCode.MessageTooLarge`.
- A record for a partition the topic does not have fails with
  `ErrorCode.UnknownTopicOrPartition`.

While the client's buffer is full, the outer effect waits for room, then
enqueues the rest of the records. A record that finds no room within the
enqueue timeout, a minute unless set with `withEnqueueTimeout`, fails with
`ErrorCode.RequestTimedOut`, and so does every record after it in the batch.
Cancelling the outer effect stops the wait.

## When some records fail

`produceAttemptEach` answers for every record, so you can tell which records
were delivered when others were not. Each `Attempted` holds a record and either
its metadata or why it was not delivered:

```scala
producer.produceAttemptEachAndAwait(batch).flatMap: attempts =>
  attempts.toList.traverse_ :
    case Attempted(record, Right(metadata)) => recordDelivered(record, metadata)
    case Attempted(record, Left(failure))   => retryLater(record, failure)
```

- If the client refuses a record as it is enqueued, that record fails with the
  refusal, and so does every record after it in the batch.
- A record that is not delivered fails neither effect. The outer effect still
  fails, with nothing enqueued, where a record cannot be serialized.

`produceAttemptEach` has the same two stages as `produce`, and
`produceAttemptEachAndAwait` and `pipeAttemptEach` match `produceAndAwait` and
`pipe`. `pipeAttemptEach` carries on past records that fail.

## Records

A `ProducerRecord` names its topic, key, and value, and optionally a partition,
a timestamp, and headers. A key or value of `None`, through the `.option`
codecs, represents a Kafka null. Tombstones are produced this way.

## Partitions

`partitionsFor` returns the partitions a topic currently has. Use it to pick a
partition to produce to. Asking about a topic the cluster does not have creates
it if the broker creates topics on request, which Kafka brokers do by default.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*

val utf8 = Serializer.utf8[IO]

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    settings <- ProducerSettings.from(client, utf8, utf8).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    found    <- KafkaClient[IO].producer(settings).use(_.partitionsFor(topic))
  yield found
```

## Acknowledgements

`acks` sets how many replicas must hold a record before the broker acknowledges
it. It defaults to `Acks.AllReplicas`. To use any other value, turn
[idempotence](#idempotence) off first:

```scala
settings.withoutIdempotence.withAcks(Acks.Leader)
```

| value | when delivery can complete | a record is lost if |
| --- | --- | --- |
| `Acks.NoAcknowledgement` | the producer sends it without waiting for a broker response | delivery fails without the caller knowing |
| `Acks.Leader` | the partition leader has it | the leader fails before a follower copies it |
| `Acks.AllReplicas` | every in-sync replica has it | every in-sync replica fails |

## Idempotence

Producers are idempotent by default: xkafka turns on Kafka's
`enable.idempotence` on every backend. See Kafka's documentation of that setting
for what it guarantees.

Idempotence requires `Acks.AllReplicas`. Settings that combine it with another
`acks` value are rejected with `SettingsError.IdempotenceRequiresAllReplicas`,
which is why `withAcks` and `withIdempotence` return a `ValidatedNel`. Call
`withoutIdempotence` to turn it off.

[Transactional producers](transactions.md) are always idempotent, because Kafka
requires it for transactions.

## Batching and compression

A producer sends records to the broker in batches. `linger` sets how long a
record waits for others to join its batch before the batch is sent. It defaults
to five milliseconds, and zero sends each batch as soon as it can. A longer
linger makes larger batches, at the cost of that much delay per record.

`compression` sets how each batch is compressed: `Compression.Uncompressed`,
which is the default, `Gzip`, `Snappy`, `Lz4` or `Zstd`. Every backend supports
all five, and consumers read them all without being told which one was used.

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def batched[F[_], K, V](settings: ProducerSettings[F, K, V]): IO[ProducerSettings[F, K, V]] =
  settings.withCompression(Compression.Zstd).withLinger(20.millis).liftTo[IO]
```

`withLinger` returns a `ValidatedNel`, because a negative linger, or one longer
than `ProducerSettings.LingerLimit`, is rejected.

A batch is sent as soon as it is full or its linger has passed, whichever comes
first. `batchSize` sets how many bytes of records make a batch full. Unless it is
set, each backend keeps its own default, and the two differ: 16384 bytes on the
JVM, and 1000000 bytes on Scala.js and Scala Native. `withBatchSize` sets the same size on every backend, and
`withoutBatchSize` goes back to each one's default.

## Flushing

`flush` sends every record enqueued before it without waiting out the linger,
and completes once each of them has been delivered or has failed:

```scala
for
  delivered <- producer.produce(batch)
  _         <- producer.flush
  results   <- delivered
yield results
```

- A record that fails does not fail the flush. Its own result reports it.
- Records enqueued while the flush waits are not waited for.
- Cancelling a flush, for example with `.timeout`, stops the wait. The records
  stay enqueued.

## Releasing a producer

Releasing the resource first waits for records the producer has already accepted,
then closes the producer. `closeTimeout` bounds that delivery wait and defaults to
sixty seconds:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*

def withCloseTimeout[F[_], K, V](settings: ProducerSettings[F, K, V]): IO[ProducerSettings[F, K, V]] =
  settings.withCloseTimeout(10.seconds).liftTo[IO]
```

Like every duration wither, it revalidates and returns
`ValidatedNel[SettingsError, ProducerSettings[F, K, V]]`. See
[Settings](settings.md).

Records still undelivered when the timeout expires are dropped. Closing the
producer comes after that wait, so releasing can take somewhat longer than
`closeTimeout`. A shorter timeout releases sooner but can drop more records.

@:callout(info)
`closeTimeout` applies to producers. Consumers have no equivalent setting.
@:@

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

`produce` returns `F[F[ProducerResult]]`. The outer effect completes once the
backend has accepted the records for delivery, and the inner one once the
backend reports their delivery outcome. With acknowledgements enabled this
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

A `ProducerResult` pairs every record with the metadata its backend reported for
it, as a `NonEmptyList[(ProducerRecord, Option[RecordMetadata])]`. A `None`
means the backend completed delivery without reporting metadata for the record.

`RecordMetadata` carries the topic-partition, and the offset and timestamp when
the backend supplies them.

## Records

A `ProducerRecord` names its topic, key, and value, and optionally a partition,
a timestamp, and headers. A key or value of `None`, through the `.option`
codecs, represents a Kafka null. Tombstones are produced this way.

## Partitions

`partitionsFor` returns the partitions a topic currently has. Use it to pick a
partition to produce to:

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
it. It defaults to `Acks.AllReplicas`. Any other value needs
[idempotence](#idempotence) turned off first:

```scala
settings.withoutIdempotence.withAcks(Acks.Leader)
```

| value | when delivery can complete | a record is lost if |
| --- | --- | --- |
| `Acks.NoAcknowledgement` | the producer sends it without waiting for a broker response | delivery fails without the caller knowing |
| `Acks.Leader` | the partition leader has it | the leader fails before a follower copies it |
| `Acks.AllReplicas` | every in-sync replica has it | every in-sync replica fails |

## Idempotence

A producer is idempotent by default: it turns on Kafka's idempotent producer,
`enable.idempotence`, on every backend. Kafka's documentation describes what that
guarantees.

An idempotent producer needs `Acks.AllReplicas`. Settings that ask for idempotence
with any other `acks` value are rejected with
`SettingsError.IdempotenceRequiresAllReplicas`, so `withAcks` and
`withIdempotence` revalidate and return
`ValidatedNel[SettingsError, ProducerSettings[F, K, V]]`. `withoutIdempotence`
turns it off.

A [transactional producer](transactions.md) is always idempotent, because Kafka
requires it of transactions.

## Releasing a producer

Releasing the resource first waits for records the producer has already accepted,
then closes the backend. `closeTimeout` bounds that delivery wait and defaults to
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

Records still undelivered when the timeout expires are dropped. Final backend
teardown happens afterwards, so the complete resource finalizer may take longer
than `closeTimeout`. Lower the timeout for a faster delivery wait, at the cost of
losing more on the way out.

@:callout(info)
`closeTimeout` applies to producers. There is no equivalent setting for
consumers: how long a consumer takes to leave its group is decided by the
platform, not by these settings.
@:@

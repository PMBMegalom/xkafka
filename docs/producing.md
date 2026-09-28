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
backend has accepted the records for delivery, and the inner one once the broker
has acknowledged them. Holding the inner effect lets several batches be in
flight at once:

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

`pipe` enqueues later batches while earlier ones are still being acknowledged,
and emits results in the order the batches arrived. `maxInFlight` sets how many
batches may await the broker at once. Mapping `produceAndAwait` over a stream
instead would send one batch at a time.

To write records and consumer offsets atomically, use a transactional producer.
See [Transactions](transactions.md).

## Results

A `ProducerResult` pairs every record with the metadata its backend reported for
it, as a `NonEmptyList[(ProducerRecord, Option[RecordMetadata])]`. A `None`
means the backend acknowledged the record but reported no metadata for it.

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
it. It defaults to `Acks.AllReplicas`:

```scala
settings.withAcks(Acks.Leader)
```

| value | the broker replies once | a record is lost if |
| --- | --- | --- |
| `Acks.NoAcknowledgement` | the record is sent | delivery fails for any reason |
| `Acks.Leader` | the partition leader has it | the leader fails before a follower copies it |
| `Acks.AllReplicas` | every in-sync replica has it | every in-sync replica fails |

## Releasing a producer

Releasing the resource delivers the records the producer has already accepted,
then closes it. `closeTimeout` sets how long that takes at most, and defaults to
sixty seconds:

```scala
settings.withCloseTimeout(10.seconds)
```

Records still undelivered when the timeout expires are dropped. Lower the
timeout for a faster shutdown, at the cost of losing more on the way out.

@:callout(info)
`closeTimeout` applies to producers. There is no equivalent setting for
consumers: how long a consumer takes to leave its group is decided by the
platform, not by these settings.
@:@

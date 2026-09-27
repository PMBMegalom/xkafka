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
has acknowledged them, so several batches can be in flight at once:

```scala
for
  first  <- producer.produce(firstBatch)
  second <- producer.produce(secondBatch)
  _      <- first
  _      <- second
yield ()
```

`produceAndAwait` combines both stages when that pipelining is not wanted.

`pipe` does the pipelining for a stream of batches, which mapping
`produceAndAwait` over one does not:

```scala
batches.through(producer.pipe(maxInFlight = 256))
```

It enqueues later batches while earlier ones are still being acknowledged, and
reports the results in the order the batches arrived. `maxInFlight` bounds how
many wait for the broker at once.

A producer that must write records and consumer offsets as one unit is a
different type. See [Transactions](transactions.md).

## Results

A `ProducerResult` pairs every record with the metadata its backend reported for
it, as a `NonEmptyList[(ProducerRecord, Option[RecordMetadata])]`. A `None`
means the backend acknowledged the record but reported no metadata for it.

`RecordMetadata` carries the topic-partition, and the offset and timestamp when
the backend supplies them.

## Partitions

`partitionsFor` reports the partitions a topic currently has, which is what
choosing one to produce to needs:

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

## Releasing a producer

Releasing the resource delivers the records the producer has already accepted
before it closes, which is what makes dropping the acknowledgement safe.
`closeTimeout` bounds that wait and defaults to sixty seconds:

```scala
settings.withCloseTimeout(10.seconds)
```

Whatever is still undelivered when it runs out is dropped, so shortening it
trades data for a faster shutdown.

@:callout(info)
This bounds a producer only. A consumer's close is bounded by each backend's own
group machinery, which is not something the portable settings can set: librdkafka
only advances a bounded close while its own consumer queue is being served, which
a released consumer is not.
@:@

## Records

A `ProducerRecord` names its topic, key, and value, and optionally a partition,
a timestamp, and headers. A key or value of `None`, through the `.option`
codecs, is how a Kafka null is expressed, which is also how tombstones are
modelled.

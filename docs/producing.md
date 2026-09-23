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

## Results

A `ProducerResult` pairs every record with the metadata its backend reported for
it, as a `NonEmptyList[(ProducerRecord, Option[RecordMetadata])]`. A `None`
means the backend acknowledged the record but reported no metadata for it.

`RecordMetadata` carries the topic-partition, and the offset and timestamp when
the backend supplies them.

## Records

A `ProducerRecord` names its topic, key, and value, and optionally a partition,
a timestamp, and headers. A key or value of `None`, through the `.option`
codecs, is how a Kafka null is expressed, which is also how tombstones are
modelled.

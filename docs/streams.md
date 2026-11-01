# Streams

`KafkaClient.streams` runs a Kafka Streams program. Each running instance is one
member of the application, and the application can mix xkafka members with Java
Kafka Streams instances running the same program. Members share the work and
take it over from each other as they start and stop.

Streams applications need brokers of Kafka 4.2 or newer.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val text = Serde.utf8[IO]

val program =
  for
    input    <- Topic.from("words").liftTo[IO]
    output   <- Topic.from("word-counts").liftTo[IO]
    pipeline  = KStream(input, text, text).groupByKey(text, text).count(Store("counts")).toStream.to(output, text, Serde.long[IO])
    topology <- Topology.of(pipeline).liftTo[IO]
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- ApplicationId.from("word-counter").liftTo[IO]
    settings <- StreamsSettings.from(client, id).liftTo[IO]
    _        <- KafkaClient[IO].streams(settings, topology).use(_.run)
  yield ()
```

Acquiring the resource joins the application and starts processing. `run` waits
while the application runs, and fails with the error that stops it. Releasing
the resource commits the member's work, hands it back to the application, and
leaves, waiting at most `closeTimeout`.

## Programs

A program is a `Topology` of pipelines, each reading an input topic and writing
an output topic. A pipeline starts from a `KStream` and is built with:

- `filter`, `map`, `mapValues`, `flatMap`, `selectKey` and `peek`;
- `evalFilter`, `evalMap` and `evalMapValues`, which run an effect;
- `groupByKey` or `groupBy`, followed by `count`, `reduce` or `aggregate` into a
  named `Store`. This gives a `KTable`, and its `toStream` is every change to it;
- `to`, which ends the pipeline at its output topic.

Grouping records whose key may have changed, after `map`, `flatMap`, `selectKey`
or `evalMap`, sends them through a repartition topic first. Name it with a
`Repartition`: `groupBy` always takes one, and `groupByKey` needs one after such
a step.

The application's own topics are named after its id: a store's changelog is
`<application id>-<store>-changelog`, and a repartition topic is
`<application id>-<name>-repartition`. The broker creates them when the
application first starts.

`Topology.of` checks the program and reports every problem at once, such as two
stores with the same name or two pipelines reading the same topic. `liftTo`
raises them in `F` as an `InvalidTopology`, and refused settings as an
`InvalidStreamsSettings`.

A `Serde` holds the serializer and deserializer of one type, since a streams
application both reads and writes its topics. `Serde.utf8`, `Serde.long` and
`Serde.bytes` write the same bytes as Java's `Serdes.String()`, `Serdes.Long()`
and `Serdes.ByteArray()`.

## Windows

Windows split each key's records by time, so you can count, reduce or aggregate
them per minute, per hour, and so on. Group the records, then window them:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def perMinute(input: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(input, text, text)
    .groupByKey(text, text)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .count(Store("counts-per-minute"))
    .toStream
    .to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])
```

This counts each key's records per minute, writing a new count each time one
changes.

`TimeWindows.tumbling` makes windows that follow one another without gaps or
overlap, so each record counts in exactly one. `TimeWindows.hopping` starts a
new window every `advance`, so windows overlap and a record counts in every
window it falls in.

Time here is the records' own timestamps, not the clock. Records often arrive
out of order, so each window keeps accepting records for a `grace` period after
it ends. Once a record timestamped at or after a window's end plus its grace has
arrived, the window is closed and later records for it are dropped. Use
`tumblingWithNoGrace` or `hoppingWithNoGrace` to close windows as soon as they
end.

The result is keyed by `Windowed[K]`: the original key and its `Window`, from
`start` up to but not including `end`, in epoch milliseconds.

To write windowed keys to a topic, use `Serde.timeWindowed(key, size)`. It
writes the same bytes as Java's `WindowedSerdes.timeWindowedSerdeFrom`, so Java
applications can read them. Only the window's start is written, which is why
reading it back needs the window size.

## Guarantees

With `ProcessingGuarantee.AtLeastOnce`, the default, a member commits its
progress every 30 seconds. If its work moves to another member, the records read
since the last commit are processed and written again.

With `ProcessingGuarantee.ExactlyOnce`, a member writes its output and commits
its progress together in one transaction, every 100ms. Each record then counts
exactly once in the application's stores and output topics, even when work
moves between members. Input is read with `IsolationLevel.ReadCommitted`.

These are the same intervals Java Kafka Streams uses. Change them with
`commitInterval`.

@:callout(warning)
Under either guarantee, an effectful step can run more than once for the same
record, such as after a member stops without committing. Exactly once applies to
what the application writes to Kafka, not to what its effects do elsewhere.
@:@

## Concurrency

By default a member processes one task at a time, like a single Java Kafka
Streams thread. Set `concurrency` higher to process up to that many tasks at
once. Records within each task stay in order. This helps when an effectful step
waits on something outside Kafka.

## State

`state` streams the application's current phase, and then each change to it:

| phase | meaning |
| --- | --- |
| `Joining` | waiting to be given work |
| `Restoring` | refilling its stores from their changelogs before processing |
| `Running` | processing |
| `Lost` | its work was taken away, and it is joining again |

Each state also lists the `problems` the broker reports, such as a missing
input topic or the application not being allowed to use its topics
(`StreamsProblem.TopicAuthorizationFailed`). Like Java Kafka Streams, a member
keeps going for as long as it can. To stop on a problem, watch for it:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.streams.*

def runUntilRefused(streams: KafkaStreams[IO]): IO[Unit] =
  val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.TopicAuthorizationFailed])).compile.drain
  IO.race(streams.run, refused).void
```

Some brokers are configured to ask applications for a description of their
topology. xkafka members don't send one, and keep working without it.

## Stores and standbys

Stores are kept in memory. A member given new work first refills its stores from
their changelogs.

If the broker's `streams.num.standby.replicas` setting asks for standby
replicas, members also keep up-to-date copies of other members' stores. When
work moves to a member holding a copy, it only needs to catch up on what the
copy has not yet read.

@:callout(info)
A copy that is no longer needed is kept for 10 minutes, like Java Kafka
Streams' state directories, in case the work comes back.
@:@

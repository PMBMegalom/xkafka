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
`start` to `end` in epoch milliseconds. A time window does not include its end;
sliding windows and sessions, below, do.

### Final results only

By default, every update to a window goes downstream. To send only each
window's final result, once the window has closed, add `emitOnWindowClose`:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def finalPerMinute(input: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(input, text, text)
    .groupByKey(text, text)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .withRetention(10.minutes)
    .emitOnWindowClose
    .count(Store("final-counts-per-minute"))
    .toStream
    .to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])
```

A closed window's result is sent when the next record arrives, at most once a
second, as in Java Kafka Streams. So results do not go out at the exact moment
a window closes, and while no records arrive, they wait.

If the work moves to another member, results that were already sent and
committed are not sent again.

By default a window is kept only for its size plus its grace, and with
`emitOnWindowClose` that can lose results. Take one-minute windows with ten
seconds of grace, which are kept for seventy seconds by default, and these
records, timestamped:

- 12:00:30, counted in the window from 12:00;
- 12:02:05, the next record.

The record at 12:02:05 closes the window from 12:00, whose grace ended at
12:01:10. But it also starts the window from 12:02, which is more than seventy
seconds after 12:00, so the window from 12:00 is removed before its result is
sent. Its count never goes downstream. The same happens when the records in
between arrive within the second after the previous look for closed windows.

Keeping windows longer, as `withRetention(10.minutes)` does in the example,
keeps the window from 12:00 until its result has been sent. Java Kafka Streams
has the same limit, and the same remedy.

### Sliding windows

Sliding windows answer questions like "how many records arrived within five
minutes of each other?". Each window covers records at most a time difference
apart. Windows are made only where records are, so there are no empty
windows:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def withinFiveMinutes(input: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(input, text, text)
    .groupByKey(text, text)
    .windowedBy(SlidingWindows(5.minutes, grace = 30.seconds))
    .count(Store("counts-within-five-minutes"))
    .toStream
    .to(output, Serde.timeWindowed(text, 5.minutes), Serde.long[IO])
```

Each record ends a window, and a window also starts just after a record when
later records follow it within the time difference. For example, with a
five-second difference, records of one key at 10:00:00 and 10:00:01 give three
windows:

- the window ending at 10:00:00, with a count of 1;
- the window ending at 10:00:01, holding both records, with a count of 2;
- the window starting just after 10:00:00, holding only the later record, with
  a count of 1.

Records that arrive late within the grace update every window they fall in.
Use `SlidingWindows.withNoGrace` for no grace.

By default, windows are kept for twice the time difference plus the grace,
because a new record can need windows that far back. That is also the shortest
`withRetention` accepts. `emitOnWindowClose` works as for time windows.

### Session windows

Session windows group each key's records into bursts of activity, such as a
user's visits: a session lasts as long as records keep arriving within an
inactivity gap of each other, and ends once the gap passes with none.

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def visits(input: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(input, text, text)
    .groupByKey(text, text)
    .windowedBy(SessionWindows(5.minutes, grace = 1.minute))
    .count(Store("records-per-visit"))
    .toStream
    .to(output, Serde.sessionWindowed(text), Serde.long[IO].option)
```

A new record can bridge sessions that were separate until then, merging them
into one, so a session already sent downstream can be replaced. That is why
session results are `Option` values: each replaced session goes downstream as
`None`, then the merged session goes with its value. For example, with a five-minute gap, records of one key
at 10:00 and 10:02 first give the session from 10:00 to 10:00 with a count of 1;
the second record then gives `None` for that session and a count of 2 for the
session from 10:00 to 10:02.

Write session results with an `option` serde, such as `Serde.long[IO].option`:
`None` is written as a record with no value, a tombstone, exactly as Java Kafka
Streams writes a replaced session.

`count` and `reduce` know how to merge sessions. `aggregate` takes a function
for it as well, given the two sessions' values:

```scala
.aggregate(Store("visit-lengths"), Serde.long[IO])(0L)((_, value, total) => total + value.length)((_, first, second) => first + second)
```

A session closes once a record arrives timestamped later than its end plus the
inactivity gap plus the grace, and later records no longer join it. By
default, sessions are kept for the gap plus the grace after they end, which is
also the shortest `withRetention` accepts. With `emitOnWindowClose`, only each
session's final value goes downstream, and no `None` values are sent.

### Writing windowed keys

To write windowed keys to a topic, use `Serde.timeWindowed(key, size)`, with the
window size for time windows and the time difference for sliding windows. It
writes the same bytes as Java's `WindowedSerdes.timeWindowedSerdeFrom`, so Java
applications can read them. Only the window's start is written, which is why
reading it back needs the size.

For sessions, use `Serde.sessionWindowed(key)`, which writes both the start and
the end, as Java's `WindowedSerdes.sessionWindowedSerdeFrom` does.

Like Java Kafka Streams, xkafka sends windowed results to the partition of their
key alone, not of the key and window together. All of a key's windows are then
in one partition, in order, and in the same partition as that key in topics
that are not windowed. Since a plain Kafka producer would hash the whole
windowed key instead, other producers writing to the same topic should do the
same as Java Kafka Streams.

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

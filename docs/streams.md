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
an output topic. A pipeline starts from a `KStream`, or a `KTable` read from a
topic (see [Tables](#tables)), and is built with:

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

## Tables

A `KTable` holds the latest value of each key. Besides the tables that `count`,
`reduce` and `aggregate` make, a table can be read straight from a topic, as Java
Kafka Streams' `StreamsBuilder.table` reads it:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def latestNames(users: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KTable(users, text, text, Store("users"))
    .toStream
    .to(output, text, text.option)
```

Each record's value replaces its key's, even a record timestamped earlier than
the one it replaces. A record without a value (a tombstone) deletes its key, and
a record without a key is skipped. The table is kept in the named store, whose
changelog is `<application id>-<store>-changelog`, here
`<application id>-users-changelog`.

A table read from a topic can delete keys, so its `toStream` gives `Option`
values: `Some` for an update and `None` for a delete, which `Serde.option`
writes as a tombstone, as Java writes its null. A table that only ever updates
its keys, such as a `count`, gives its values as they are. The table's last type
parameter says which: `KTable[F, K, V, Option]` can delete, and
`KTable[F, K, V, KTable.Updated]` only updates. Writing a table that can delete
without `.option` does not compile.

## Joins

`join` pairs each record of a stream with its key's value in a table, as Java
Kafka Streams' stream-table join does:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithUsers(orders: Topic, users: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(orders, text, text)
    .join(KTable(users, text, text, Store("users")))((order, user) => s"$order for $user")
    .to(output, text, text)
```

Each record is joined with the value the table holds for its key when the record
is processed; later changes to the table do not change records already joined.
A record whose key the table holds no value for, never set or deleted, is
dropped. `leftJoin` keeps it, passing `None` instead of the table's value. The
result keeps each record's timestamp and headers.

The table can be any table with a store: read from a topic, or made by `count`,
`reduce` or `aggregate`. Joining a suppressed table sees, for a key the
suppression holds, the value from before the change it holds, as Java does.
Windowed tables cannot be joined yet.

A stream and the table it joins are processed together, so their topics must
have the same number of partitions, as in Java. If they do not, the broker
refuses the application's topics, which shows up in `state` as
`StreamsProblem.IncorrectlyPartitionedTopics`. A member processes the records
it has read from both topics earliest timestamp first, as Java does.

An application whose members all run xkafka can run any join. When some of its
members run Java Kafka Streams, the broker requires every member to describe the
application exactly as Java does. For a join this includes the order of the
joined topics, which Java takes from a hash of their names, and for some pairs
of names from internal names xkafka cannot know. For such a pair, the broker may
refuse the Java members. xkafka tells you in advance: the application's `state`
lists `StreamsProblem.JavaMembersMayBeRefused`, naming the topics. Joining
topics with other names avoids it.

If the stream's key changed since it was read, for example after `selectKey`,
its records first go through a repartition topic, which `join` and `leftJoin`
then need a name for:

```scala
orders
  .selectKey((_, order) => userOf(order))
  .join(users, Repartition("orders-by-user"), text, order)((order, user) => s"$order for $user")
```

### Joining tables

Two tables with the same key join into a table, as Java Kafka Streams' table-table
joins do. Every change to either table updates the result for its key:

```scala mdoc:compile-only
import cats.data.Ior
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def accounts(users: Topic, plans: Topic, output: Topic): Pipeline[IO] =
  val text  = Serde.utf8[IO]
  val named = KTable(users, text, text, Store("users"))
  val paid  = KTable(plans, text, text, Store("plans"))
  named
    .outerJoin(paid) {
      case Ior.Left(user)       => s"$user, no plan"
      case Ior.Right(plan)      => s"unknown user on $plan"
      case Ior.Both(user, plan) => s"$user on $plan"
    }
    .toStream
    .to(output, text, text.option)
```

- `join` holds a value for each key both tables hold a value for.
- `leftJoin` holds one for each key the left table holds, passing the right
  table's value as an `Option`.
- `outerJoin` holds one for each key either table holds, passing the values as
  an `Ior`: the left, the right, or both.

The result can delete keys, whichever tables were joined, so its changes are
`Option` values, and `toStream` writes a delete as a tombstone. As in Java, a
delete on either side of `join` deletes the key from the result, even a key the
result never held. The joining function cannot delete a key itself. Each
result takes the later timestamp of the two values it was made from.

The result is computed when needed and kept in no store. To keep it in one, pass
the store's name and a serde for its values; a table that holds the result back
with `suppress` needs this:

```scala
users
  .join(plans, Store("accounts"), text)((user, plan) => s"$user on $plan")
  .suppress(Suppressed.untilTimeLimit("accounts-calm", 1.second))
```

Like a stream and a table, two joined tables are processed together, so their
topics must have the same number of partitions. A table cannot be joined with
itself, or with a table read from the same topic, and windowed tables cannot be
joined yet; `Topology.of` refuses each with a `TopologyError`.

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
into one, so a session already sent downstream can be replaced. A table of
sessions is therefore a table that can delete keys (see [Tables](#tables)): its
`toStream` gives `None` for each replaced session, then the merged session with
its value. For example, with a five-minute gap, records of one key at 10:00 and
10:02 first give the session from 10:00 to 10:00 with a count of 1; the second
record then gives `None` for that session and a count of 2 for the session from
10:00 to 10:02.

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

## Holding back updates

`suppress` holds back a table's updates and sends on only some of them, as Java
Kafka Streams' `suppress` does. Each suppression has a name, and keeps what it
holds in a store called `<name>-store`, so a member that takes over the work
goes on with what the last one held.

To send only each window's final result, use `Suppressed.untilWindowCloses`:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def finalCounts(input: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(input, text, text)
    .groupByKey(text, text)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .count(Store("counts-per-minute"))
    .suppress(Suppressed.untilWindowCloses("final-counts"))
    .toStream
    .to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])
```

A window's result goes on once a record arrives timestamped at or after the
window's end plus its grace. For sessions, the inactivity gap is added too,
and a session merged into another is not sent at all. `untilWindowCloses` is
only available on windowed tables.

To send each key's latest value at most once in a while, use
`Suppressed.untilTimeLimit`, on any table:

```scala
.count(Store("counts"))
.suppress(Suppressed.untilTimeLimit("calm-counts", 1.minute))
```

Once a key has an update, it is held. Later updates replace it. The latest goes
on once a record arrives timestamped a minute or more after the first. Time
here is the records' time, so while no records arrive, nothing goes on.

### Buffer limits

By default a suppression holds as much as it needs to. To limit it, pass a
`BufferLimit`:

```scala
Suppressed.untilTimeLimit("calm-counts", 1.minute, BufferLimit.records(10000))
```

`BufferLimit.records(n)` holds at most `n` keys, and `BufferLimit.bytes(n)` at
most `n` bytes, counted as Java Kafka Streams counts them. When the buffer holds
more, the earliest updates go on early. To stop the application with an error
instead, add `shutDownWhenFull`. `untilWindowCloses` only accepts limits that
never send early: none, or one with `shutDownWhenFull`.

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

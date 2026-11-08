# Processors

A processor is a step you write yourself, for logic that the other steps cannot
express. It sees one record at a time with its time and headers, can read and
write its own stores, and returns the records to send on.

```scala mdoc:compile-only
// Keep a running total per user, and send on each new total
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def runningTotals(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val totals = KeyValueStore.of(Store("totals"), user, amount)  // Declare the store
  KStream(Named("payments"), input, user, amount)
    .process(
      Named("add-to-total"),
      Processor.withStore(totals): store =>        // Give the processor the store
        Processor.onRecord: record =>              // Runs for each record
          store.get(record.key).flatMap: total =>  // Read this user's total
            val updated = total.getOrElse(0L) + record.value
            store.put(record.key, updated).as(List(record.withValue(updated)))  // Save the new total, and send it on
    )
    .to(Named("write-totals"), output, user, amount)
```

`Processor.withStore` hands your code the store already typed, so you cannot
read a store you did not declare. Use `Processor.withStores` for two or three
stores, and `Processor.withoutStores` for none.

## What comes out

Each record you return goes on to the next step. A record made with `withKey` or
`withValue` keeps the time and headers of the one it was made from, and
`withTimestamp` and `withHeaders` change them. Return an empty list to send
nothing, or several records to send them all, in order.

The records you read also say where they came from: `record.origin` gives the
topic, partition and offset.

Since a processor can give a record any key, grouping its results needs a
repartition topic, so `groupByKey` asks you to name one.

## Keeping the key

If your processor never changes the key, use `processValues` with a
`FixedKeyProcessor`. Its records have `withValue`, `withTimestamp` and
`withHeaders` but no `withKey`, so a change of key does not compile.

```scala mdoc:compile-only
// Number each user's payments, then count them, without ever changing the key
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def numbered(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val seen   = KeyValueStore.of(Store("seen"), user, amount)
  KStream(Named("payments"), input, user, amount)
    .processValues(  // A step that cannot change the key
      Named("number-payments"),
      FixedKeyProcessor.withStore(seen): store =>
        FixedKeyProcessor.onRecord: record =>
          store.get(record.key).flatMap: last =>
            val number = last.getOrElse(0L) + 1
            store.put(record.key, number).as(List(record.withValue(number)))  // This record has no withKey, so the key cannot change
    )
    .groupByKey(user, amount)  // The key did not change, so this needs no repartition topic
    .count(Named("count-payments"), Store("counts"))
    .toStream(Named("counts-stream"))
    .to(Named("write-counts"), output, user, Serde.long[IO])
```

Since the key stays, `groupByKey` needs no repartition topic here, unless an
earlier step changed the key. `FixedKeyProcessor` declares its stores with
`withStore`, `withStores` and `withoutStores`, and its stores work as described
above.

## Stores

A `KeyValueStore` has `get`, `put`, `putIfAbsent`, `putAll` and `delete`, and
reads many keys with `all`, `range`, `prefixScan`, `reverseAll` and
`reverseRange`, which give an `fs2.Stream`.

Those reads follow the order of the keys' serialized bytes, which is not always
the order of the keys themselves. A number written as text, for example, sorts
`"10"` before `"9"`.

Your code reads what it has written straight away. The writes are saved at
each commit, along with the records you returned and the offsets read, to a topic
named `<application id>-<store>-changelog`. A member that takes over the work
restores the store from it. If your handler fails, nothing it wrote for that
record is saved.

Anything else your handler does, such as calling another service, runs again for
records that are processed again after a crash or when another member takes over.

## Window stores

A `WindowStore` holds one value for each key and window, and forgets windows once
they are old enough. You choose each window's start, usually by rounding a record's
time down. This one counts each user's failed logins per minute, and sends on the
total for the last ten minutes once it passes five:

```scala mdoc:compile-only
// Count failed logins per user and minute, and flag a user with over five in ten minutes
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def repeatedFailures(input: Topic, output: Topic): Pipeline[IO] =
  val user     = Serde.utf8[IO]
  val count    = Serde.long[IO]
  // Holds one count per user and minute, and keeps each for an hour
  val failures = WindowStore.of(Store("failures"), user, count, size = 1.minute, retention = 1.hour)
  KStream(Named("failed-logins"), input, user, Serde.utf8[IO])
    .process(
      Named("flag-repeats"),
      Processor.withStore(failures): store =>
        Processor.onRecord: record =>
          val now    = record.timestamp.epochMillis
          val minute = Timestamp.fromEpochMillis(now - now % 60000)  // Round down to the start of this minute's window
          for
            seen   <- store.fetch(record.key, minute)                        // This minute's count so far
            _      <- store.put(record.key, seen.getOrElse(0L) + 1, minute)  // Save it under this minute's window
            // Add up the counts of the last ten minutes
            recent <- store.fetch(record.key, Timestamp.fromEpochMillis(now - 600000), record.timestamp).map(_._2).compile.foldMonoid
          yield if recent > 5 then List(record.withValue(recent)) else Nil   // Send the total on only once it passes five
    )
    .to(Named("write-flags"), output, user, count)
```

| To read | Use | You get |
| --- | --- | --- |
| One key's window | `fetch(key, start)` | an `Option` of its value |
| One key's windows between two starts | `fetch(key, from, to)`, `reverseFetch` | each window's start and value |
| A range of keys between two starts | `fetchRange(first, last, from, to)`, `reverseFetchRange` | each `Windowed` key and value |
| Every key between two starts | `fetchAll(from, to)`, `reverseFetchAll` | each `Windowed` key and value |
| Everything | `all`, `reverseAll` | each `Windowed` key and value |

Both ends of a range are included. Reads over several keys go by window start, then
by the order of the keys' serialized bytes, and the `reverse` reads give the exact
opposite order. `delete(key, start)` removes one window.

`retention` decides how long a window lasts. A window has expired once the latest
start written to the store, by a `put` or a `delete`, is a whole retention past its
own start. Reads do not find an expired window, and a `put` to one keeps nothing and
raises no error, so choose a retention at least as long as the oldest window you
still write to or read.

`size` is only what the `end` of a `Windowed` key says: a `put` does not check that
starts are a size apart. A size under a millisecond, or a retention shorter than the
size, is refused when you call `Topology.of`.

Writes are saved and restored as for a `KeyValueStore`, to the topic
`<application id>-<store>-changelog`, which keeps records for the retention plus a
day.

## Session stores

A `SessionStore` holds one value for each session: a key with a start and an end.
You decide what a session is, and how sessions merge. This one counts each user's
page views per visit, where a visit ends after five minutes with no views. A view
within five minutes of two visits joins them into one:

```scala mdoc:compile-only
// Count each user's page views per visit. A view within five minutes of two visits joins them into one
import scala.concurrent.duration.*

import cats.effect.IO
import cats.syntax.all.*

import xkafka.*
import xkafka.streams.*

def viewsPerVisit(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val count  = Serde.long[IO]
  val visits = SessionStore.of(Store("visits"), user, count, retention = 1.hour)  // Holds one count per user and visit, and keeps each visit for an hour
  val gap    = 5.minutes.toMillis                                                 // A visit ends after five minutes with no views
  KStream(Named("page-views"), input, user, Serde.utf8[IO])
    .process(
      Named("count-visits"),
      Processor.withStore(visits): store =>
        Processor.onRecord: record =>
          val at = record.timestamp.epochMillis
          for
            // Find this user's visits within five minutes of this view
            nearby <- store.findSessions(record.key, Timestamp.fromEpochMillis(at - gap), Timestamp.fromEpochMillis(at + gap)).compile.toList
            _      <- nearby.traverse_((session, _) => store.remove(session))     // Remove those visits, because one merged visit replaces them
            start   = (at :: nearby.map(_._1.window.start)).min                   // The merged visit starts at the earliest start
            end     = (at :: nearby.map(_._1.window.end)).max                     // The merged visit ends at the latest end
            total   = nearby.map(_._2).sum + 1                                    // Its count is their counts plus this view
            _      <- store.put(Windowed(record.key, Window(start, end)), total)  // Save the merged visit
          yield List(record.withValue(total))
    )
    .to(Named("write-visits"), output, user, count)
```

| To read | Use |
| --- | --- |
| One exact session | `fetchSession(key, start, end)` |
| Every session of a key | `fetch(key)`, `reverseFetch` |
| Every session of a range of keys | `fetchRange(first, last)`, `reverseFetchRange` |
| A key's sessions overlapping a stretch of time | `findSessions(key, earliestEnd, latestStart)`, `reverseFindSessions` |
| The same for a range of keys | `findSessionsRange(first, last, earliestEnd, latestStart)`, `reverseFindSessionsRange` |
| Every key's sessions ending within a stretch of time | `findSessionsEnding(earliestEnd, latestEnd)` |

Reads go by each session's end, then by the order of the keys' serialized bytes, then
with the latest start first, and the `reverse` reads give the exact opposite order.
Both ends of a range are included, and a range the wrong way round finds nothing. A
`put` or `remove` of a session that ends before it starts fails your handler.

`retention` decides how long a session lasts. A session has expired once the latest
end written to the store is a whole retention past its own end. Reads do not find an
expired session, and a `put` to one keeps nothing and raises no error, so choose a
retention at least as long as the oldest session you still write to or read.

A `remove` does not count towards the latest end while
the store runs, but it does once the store is restored. So a restart can expire a
session you could read just before it, if you removed a session ending later than
any you have written. Java Kafka Streams' session stores do the same.

A retention under a millisecond is refused when you call `Topology.of`. Writes are
saved and restored as for a `KeyValueStore`, to the topic
`<application id>-<store>-changelog`, which keeps records for the retention plus a
day.

## Running on a schedule

A punctuator is code a processor runs on a schedule, between records. Wrap the
handler in `Processor.every` with a schedule and what to do. It is given the
time it runs at, and the records it returns go on to the next step, as a
handler's do.

```scala mdoc:compile-only
// Add up each user's payments, and send all the totals each time record timestamps pass another minute
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def totalsEveryMinute(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val totals = KeyValueStore.of(Store("totals"), user, amount)
  KStream(Named("payments"), input, user, amount)
    .process(
      Named("report-totals"),
      Processor.withStore(totals): store =>
        Processor.every(Punctuation.streamTime(1.minute)): at =>                         // The schedule, and what to do each time
          store.all.compile.toList.map(_.map((name, total) => Record(name, total, at)))  // Send on every total in this task's part of the store
        .apply:
          Processor.onRecord: record =>  // Runs for each record
            store.get(record.key).flatMap: total =>
              store.put(record.key, total.getOrElse(0L) + record.value).as(Nil)  // Save the total, and send nothing on
    )
    .to(Named("write-totals"), output, user, amount)
```

A punctuator runs separately in each [task](running.md#tasks), and reads only
that task's part of the stores. Nest `every` to declare several. There are two
schedules:

| Schedule | Goes by | Runs |
| --- | --- | --- |
| `Punctuation.streamTime(interval)` | The latest record timestamp the task has read, its stream time | With the first record, then whenever stream time reaches the next multiple of the interval. Not while no records arrive |
| `Punctuation.wallClock(interval)` | The clock | Each time the interval passes, from when the task starts, whether or not records arrive |

With `Punctuation.streamTime(1.minute)` and a first record timestamped 12:00:30,
it runs at 12:00:30, and next for the first record timestamped 12:01:00 or later.
If several intervals have passed, it runs once. It is given the stream time or
clock time it runs at. An interval below a millisecond is refused when you call
`Topology.of`.

What a punctuator writes to a store is saved at the next commit, as a handler's
writes are. Under exactly-once processing, a wall-clock punctuator that runs while no records arrive
commits a transaction each time, so avoid very short intervals.

Schedules are not saved. When a task starts again, here or on another member, a
stream-time punctuator runs as soon as the task has a stream time, even if its
interval has not passed, and a wall-clock interval starts afresh. To remember when
something is due, keep it in a store.

A `FixedKeyProcessor`'s punctuators, declared with `FixedKeyProcessor.every`, can
write to its stores but cannot send records on. To send records on a schedule, use
`Processor`.

## Sharing a store

Give two processors the same `KeyValueStore.of` value and they share one store.
One task then handles the same-numbered partition of both inputs, and both
processors see the same part of the store. Two separate values with the same store
name are refused when you call `Topology.of`.

## Java processors (JVM only)

On the JVM, a `Processor` or `FixedKeyProcessor` written for Java Kafka Streams can
run as a step. It comes with the `xkafka-streams-java` module (see
[Java partitioners](README.md#java-partitioners-jvm-only)).

```scala mdoc:compile-only
// Run a processor written for Java Kafka Streams as a step, with the store it uses
import cats.effect.IO
import org.apache.kafka.streams.processor.api.Processor as JavaProcessor

import xkafka.*
import xkafka.streams.*
import xkafka.streams.interop.*                                        // Adds Processor.fromJava and JavaStore, on the JVM only

def withJava(input: Topic, output: Topic, make: () => JavaProcessor[String, String, String, String]): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val counts = JavaStore.keyValue(Store("counts"), text, text)         // The Java code finds it with getStateStore("counts")
  KStream(Named("read"), input, text, text)
    .process(Named("count"), Processor.fromJava[IO](make, counts))     // Each task makes an instance of its own with make
    .to(Named("write"), output, text, text)
```

The processor runs as it does in Java Kafka Streams:

- Each task makes its own instance, calls `init` when the task starts, and `close`
  when the task stops or moves to another member. Fields keep their values in
  between.
- `forward` sends records on, with the times and headers they are given.
- `schedule` runs punctuators by stream time or by the clock, from the start time
  given where there is one, and `cancel` stops them. Each task's instance schedules
  its own.
- `recordMetadata` gives the topic, partition and offset of the record being read,
  and `taskId`, `applicationId`, `currentStreamTimeMs` and `currentSystemTimeMs`
  give what they do in Java.

Declare each store the Java code reads with `JavaStore.keyValue`, which it takes as a
`KeyValueStore<K, V>`, or `JavaStore.timestamped`, which it takes as a
`TimestampedKeyValueStore<K, V>`. Keys and values must be objects, such as `String`
or `java.lang.Long`. Window and session stores cannot be given yet, and a store given
to a Java processor cannot also be given to one of xkafka's.

Each call into the Java code runs on a thread meant for waiting, since its calls to
stores wait for them. That can cost a switch of threads for each record such a step
handles; xkafka's own steps do not pay it.

These fail with an error saying so: writing to a store or sending records on in
`init`, sending records to a named next step, `stateDir`, `keySerde` and `valueSerde`.
`commit()` does nothing, since commits happen at the commit interval anyway, and
sensors from `metrics()` record what they are given, which nothing reports.

If some members run Java Kafka Streams, give their program the same processor under
the same name, with the same stores.


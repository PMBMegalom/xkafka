# Processors

A processor is a step you write yourself, for logic that the other steps cannot
express. It sees one record at a time with its time and headers, can read and
write its own stores, and returns the records to send on.

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def runningTotals(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val totals = KeyValueStore.of(Store("totals"), user, amount)
  KStream(Named("payments"), input, user, amount)
    .process(
      Named("add-to-total"),
      Processor.withStore(totals): kept =>
        Processor.onRecord: record =>
          kept.get(record.key).flatMap: total =>
            val updated = total.getOrElse(0L) + record.value
            kept.put(record.key, updated).as(List(record.withValue(updated)))
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
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def numbered(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val seen   = KeyValueStore.of(Store("seen"), user, amount)
  KStream(Named("payments"), input, user, amount)
    .processValues(
      Named("number-payments"),
      FixedKeyProcessor.withStore(seen): kept =>
        FixedKeyProcessor.onRecord: record =>
          kept.get(record.key).flatMap: last =>
            val number = last.getOrElse(0L) + 1
            kept.put(record.key, number).as(List(record.withValue(number)))
    )
    .groupByKey(user, amount)
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
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def repeatedFailures(input: Topic, output: Topic): Pipeline[IO] =
  val user     = Serde.utf8[IO]
  val count    = Serde.long[IO]
  val failures = WindowStore.of(Store("failures"), user, count, size = 1.minute, retention = 1.hour)
  KStream(Named("failed-logins"), input, user, Serde.utf8[IO])
    .process(
      Named("flag-repeats"),
      Processor.withStore(failures): kept =>
        Processor.onRecord: record =>
          val now    = record.timestamp.epochMillis
          val minute = Timestamp.fromEpochMillis(now - now % 60000)
          for
            seen   <- kept.fetch(record.key, minute)
            _      <- kept.put(record.key, seen.getOrElse(0L) + 1, minute)
            recent <- kept.fetch(record.key, Timestamp.fromEpochMillis(now - 600000), record.timestamp).map(_._2).compile.foldMonoid
          yield if recent > 5 then List(record.withValue(recent)) else Nil
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

## Running on a schedule

A punctuator is code a processor runs on a schedule, between records. Wrap the
handler in `Processor.every` with a schedule and what to do. It is given the
time it runs at, and the records it returns go on to the next step, as a
handler's do.

```scala mdoc:compile-only
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
      Processor.withStore(totals): kept =>
        Processor.every(Punctuation.streamTime(1.minute)): at =>
          kept.all.compile.toList.map(_.map((name, total) => Record(name, total, at)))
        .apply:
          Processor.onRecord: record =>
            kept.get(record.key).flatMap: total =>
              kept.put(record.key, total.getOrElse(0L) + record.value).as(Nil)
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
writes are, and reaches the changelog with the time it was given. Under
exactly-once processing, a wall-clock punctuator that runs while no records arrive
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
processors see the same part of the store. Two separate values with the same store name are refused when you call
`Topology.of`.

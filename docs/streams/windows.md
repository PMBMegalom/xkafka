# Windows

Windows split each key's records by time, so you can count, reduce or aggregate
them per minute, per hour, and so on. Group the records, then window them:

```scala mdoc:compile-only
// Count each user's page views per minute, and write every new count
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def viewsPerMinute(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))  // One-minute windows, which accept late records for 10 more seconds
    .count(Named("count-views"), Store("views-per-minute"))          // Count per user and window
    .toStream(Named("view-counts"))
    // Write each key together with its window
    .to(Named("write-view-counts"), output, Windowed.timeSerde(user, 1.minute), Serde.long[IO])
```

This counts each user's page views per minute, writing a new count each time
one changes.

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

## Final results only

By default, every update to a window goes downstream. To send only each
window's final result, once the window has closed, add `emitOnWindowClose`:

```scala mdoc:compile-only
// Count each user's page views per minute, and write only each final count
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def finalViewsPerMinute(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .withRetention(10.minutes)  // Keep windows until their results have been sent
    .emitOnWindowClose          // Send each result once its window closes
    .count(Named("count-views"), Store("final-views-per-minute"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, Windowed.timeSerde(user, 1.minute), Serde.long[IO])
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
sent. Its count never goes downstream. The same can happen when the records in
between arrive quickly, since closed windows are looked for at most once a second.

Keeping windows longer, as `withRetention(10.minutes)` does in the example,
keeps the window from 12:00 until its result has been sent. Java Kafka Streams
has the same limit, and the same remedy.

## Sliding windows

Sliding windows answer questions like "how many times did this user fail to
log in within five minutes?". Each window covers records at most a time difference
apart. Windows are made only where records are, so there are no empty
windows:

```scala mdoc:compile-only
// Count each user's failed logins within any five minutes
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def failedLogins(input: Topic, output: Topic): Pipeline[IO] =
  val user    = Serde.utf8[IO]
  val address = Serde.utf8[IO]
  KStream(Named("failed-logins"), input, user, address)
    .groupByKey(user, address)
    .windowedBy(SlidingWindows(5.minutes, grace = 30.seconds))  // Records at most five minutes apart share a window
    .count(Named("count-failures"), Store("failures-within-five-minutes"))
    .toStream(Named("failure-counts"))
    .to(Named("write-failure-counts"), output, Windowed.timeSerde(user, 5.minutes), Serde.long[IO])
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

## Session windows

Session windows group each key's records into bursts of activity, such as a
user's visits: a session lasts as long as records keep arriving within an
inactivity gap of each other, and ends once the gap passes with none.

```scala mdoc:compile-only
// Count each user's page views per visit
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def visits(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(SessionWindows(5.minutes, grace = 1.minute))  // A visit ends after five minutes with no views
    .count(Named("count-views"), Store("views-per-visit"))
    .toStream(Named("view-counts"))
    // A replaced session is written as a tombstone
    .to(Named("write-view-counts"), output, Windowed.sessionSerde(user), Serde.long[IO].option)
```

A new record within the gap of two separate sessions merges them into one, so a
session already sent downstream can be replaced. A table of
sessions is therefore a table that can delete keys (see [Tables](tables.md)): its
`toStream` gives `None` for each replaced session, then the merged session with
its value. For example, with a five-minute gap, records of one key at 10:00 and
10:02 first give the session from 10:00 to 10:00 with a count of 1; the second
record then gives `None` for that session and a count of 2 for the session from
10:00 to 10:02.

Write session results with an `option` serde, such as `Serde.long[IO].option`:
`None` is written as a record with no value, a tombstone, exactly as Java Kafka
Streams writes a replaced session.

`count` and `reduce` know how to merge sessions. `aggregate` takes a function
for it as well, given the two sessions' values. Over a stream of purchases in
cents, keyed by user, this adds up what each user spends per visit:

```scala
// Add up spending per visit. The last function combines two visits when they merge
.aggregate(Named("add-spending"), Store("spending-per-visit"), Serde.long[IO])(0L)((_, cents, total) => total + cents)((_, first, second) => first + second)
```

A session closes once a record arrives timestamped later than its end plus the
inactivity gap plus the grace, and later records no longer join it. By
default, sessions are kept for the gap plus the grace after they end, which is
also the shortest `withRetention` accepts. With `emitOnWindowClose`, only each
session's final value goes downstream, and no `None` values are sent.

## Joining windowed tables

Two windowed tables join window by window, as other tables join key by key. This
joins each page's clicks per minute with its views in the same minute:

```scala mdoc:compile-only
// Each page's clicks and views per minute, joined minute by minute into a click rate
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def clickRates(clicks: Topic, views: Topic, output: Topic): Pipeline[IO] =
  val page    = Serde.utf8[IO]
  val minute  = TimeWindows.tumbling(1.minute, grace = 10.seconds)
  val clicked = KStream(Named("clicks"), clicks, page, page).groupByKey(page, page).windowedBy(minute).count(Named("count-clicks"), Store("clicks"))
  val viewed  = KStream(Named("views"), views, page, page).groupByKey(page, page).windowedBy(minute).count(Named("count-views"), Store("views"))
  clicked
    .join(Named("click-rate"), viewed)((c, v) => s"${c.toDouble / v}")  // Each page and minute meets the same page and minute
    .toStream(Named("rates"))
    .to(Named("write-rates"), output, Windowed.timeSerde(page, 1.minute), page.option)
```

A window meets only the same window of the other table: the same key and the same
start, and for sessions the same end too. A window the other table has already
dropped, once its retention has passed, finds nothing. `join`, `leftJoin` and
`outerJoin` all work this way, and so does keeping the result in a store. A stream
keyed by window, such as a windowed table's `toStream`, joins a windowed table in the
same way.

Both tables' topics must have the same number of partitions, as for any table join.
Windowed tables cannot join on a foreign key.

## Writing windowed keys

To write windowed keys to a topic, use `Windowed.timeSerde(key, size)`, with the
window size for time windows and the time difference for sliding windows. It
writes the same bytes as Java's `WindowedSerdes.timeWindowedSerdeFrom`, so Java
applications can read them. Only the window's start is written, which is why
reading it back needs the size.

For sessions, use `Windowed.sessionSerde(key)`, which writes both the start and
the end, as Java's `WindowedSerdes.sessionWindowedSerdeFrom` does.

Like Java Kafka Streams, xkafka sends windowed results to the partition of their
key alone, not of the key and window together. All of a key's windows are then
in one partition, in order, and in the same partition as that key in topics
that are not windowed. A plain Kafka producer hashes the whole windowed key
instead, so other producers writing to the same topic need to choose the partition
from the key alone too.

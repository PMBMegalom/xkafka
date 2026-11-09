# Holding back updates

`suppress` holds back a table's updates and sends on only some of them, as Java
Kafka Streams' `suppress` does. Each suppression has a name, and keeps what it
holds in a store called `<name>-store`, so a member that takes over the work
goes on with what the last one held.

To send only each window's final result, use `Suppressed.untilWindowCloses`:

```scala mdoc:compile-only
// Count each user's views per minute, and write only each window's final count
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def finalViews(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .count(Named("count-views"), Store("views-per-minute"))
    .suppress(Suppressed.untilWindowCloses("final-views"))  // Hold each window's updates until it closes
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, Serde.timeWindowed(user, 1.minute), Serde.long[IO])
```

A window's result goes on once a record arrives timestamped at or after the
window's end plus its grace. For sessions, the inactivity gap is added too,
and a session merged into another is not sent at all. `untilWindowCloses` is
only available on windowed tables.

To send each key's latest value at most once in a while, use
`Suppressed.untilTimeLimit`, on any table:

```scala
// Send each page's latest count at most once a minute
.count(Named("count-views"), Store("views-per-page"))
.suppress(Suppressed.untilTimeLimit("calm-views", 1.minute))
```

Once a key has an update, it is held. Later updates replace it. The latest goes
on once a record arrives timestamped a minute or more after the first. Time
here is the records' time, so while no records arrive, nothing goes on.

## Buffer limits

By default a suppression holds as much as it needs to. To limit it, pass a
`BufferLimit`:

```scala
// Hold at most 10000 keys. Past that, the earliest updates are sent early
Suppressed.untilTimeLimit("calm-views", 1.minute, BufferLimit.records(10000))
```

`BufferLimit.records(n)` holds at most `n` keys, and `BufferLimit.bytes(n)` at
most `n` bytes, counted as Java Kafka Streams counts them. When the buffer holds
more, the earliest updates go on early. To stop the application with an error
instead, add `shutDownWhenFull`. `untilWindowCloses` only accepts limits that
never send early: none, or one with `shutDownWhenFull`.

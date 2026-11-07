# Branching

A stream is a description of steps, so each use of it is another copy of them all.
A stream used twice would read its topic twice, and `Topology.of` refuses that with
a `TopologyError`. To send records down several paths, say so where the stream
branches.

`fork` sends each record down the first path, including everything that follows
on it, then down the next, so a later path sees what an earlier one did to a store.
Only then does the next record start. Java runs the paths of a branching stream in
the same way. Each path starts from the stream's records and ends in a topic or a
table:

```scala mdoc:compile-only
// Send every order to one topic, and a marked copy of it to an audit topic
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def audited(orders: Topic, all: Topic, audit: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text).fork(  // Each record goes down these paths in turn
    _.to(Named("all"), all, text, text),              // First path: write the order as it is
    // Second path: mark the order, then write it
    _.mapValues(Named("mark"))(order => s"audit: $order").to(Named("audit"), audit, text, text)
  )
```

`forkMerge` works like `fork`, and what the paths make goes on as one stream.

`merge` makes one stream of two, and their topics need not have the same number
of partitions. A merge is keyed as read only if each of its
sides is:

```scala mdoc:compile-only
// Merge orders and refunds, flag large amounts, and write them all to one topic
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def movements(orders: Topic, refunds: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text)
    .merge(Named("movements"), KStream(Named("refunds"), refunds, text, text))  // One stream from both topics
    .forkMerge(Named("marked"))(                                                // The records of both paths continue as one stream
      _.filter(Named("large"))((_, amount) => amount.length > 3).mapValues(Named("flag"))(amount => s"large $amount"),
      _.filterNot(Named("small"))((_, amount) => amount.length > 3)
    )
    .to(Named("write"), output, text, text)
```

`split` sends each record down the first branch that takes it, as Java's `split`
does. A branch's node is named by the split's name followed by the branch's, here
`route-large`. `defaultBranch` takes what no branch took; with `noDefaultBranch`,
those records go no further:

```scala mdoc:compile-only
// Send large orders to one topic, and the rest to another
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def routed(orders: Topic, large: Topic, small: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text)
    .split(Named("route-"))                                                        // Each branch's step name starts with this
    // Large orders take this branch
    .branch(Named("large"))((_, amount) => amount.length > 3)(_.to(Named("write-large"), large, text, text))
    .defaultBranch(Named("small"))(_.to(Named("write-small"), small, text, text))  // Everything else takes this branch
```

## Paths that share what follows

`fork` writes each path once, so it cannot send two paths to the same place. For
that, write the shared part first as a flow, then send records to it with `into`.
A flow starts from one of:

- `Flow.asRead[F, K, V]`, for records keyed as they were read;
- `Flow[F, K, V]`, for records whose key may have changed;
- `s.flow`, for the records of the stream `s`, whose types and key it takes on.

`into` sends each record to any number of flows, in the order given, one finishing
before the next starts, as `fork` does. Java goes by the order the paths were
written in, so to match a Java program, list them in that order.

Here large orders and every refund go to both of two topics:

```scala mdoc:compile-only
// Send large orders and every refund to both of two topics
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def reviewed(orders: Topic, refunds: Topic, all: Topic, review: Topic) =
  val text     = Serde.utf8[IO]
  // A flow that writes to the topic all
  val toAll    = Flow.asRead[IO, String, String].merged(Named("all")).to(Named("write-all"), all, text, text)
  // A flow that writes to the topic review
  val toReview = Flow.asRead[IO, String, String].merged(Named("review")).to(Named("write-review"), review, text, text)
  // A flow that keeps large amounts, and sends them to both of the above
  val large    = Flow.asRead[IO, String, String].filter(Named("large"))((_, amount) => amount.length > 3).into(toAll, toReview)
  Topology.of(
    KStream(Named("orders"), orders, text, text).into(large),             // Orders go through the flow that keeps large amounts
    KStream(Named("refunds"), refunds, text, text).into(toAll, toReview)  // Every refund goes straight to both topics' flows
  )
```

When a flow gets records from several places, they all go straight to its first
step. `merged` adds a step of its own to receive them, as `merge` would. Use it when the
program's description must match a Java program that merges those paths.

## Joining a stream or a table with itself

Flows are also how a stream is joined with something made from itself, such as its
own count, or with itself. `consumer` ends a table with no further steps, so that
it can be sent to while a join reads it. Here the count's path comes first, so the
join sees a count that includes the record it is joining:

```scala mdoc:compile-only
// Two programs: one joins each click with its key's count, the other with clicks close to it in time
import scala.concurrent.duration.*
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def withCounts(clicks: Topic, output: Topic): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val read   = KStream(Named("clicks"), clicks, text, text)
  val counts = read.flow.groupByKey(text, text).count(Named("counted"), Store("counts"))  // Count the clicks, as a flow the stream can send to
  read.into(
    counts.consumer,  // This path runs first, so the count includes the current click
    // Join each click with its key's count
    read.flow.join(Named("with-count"), counts)((click, seen) => s"$click, seen $seen").to(Named("write"), output, text, text)
  )

def pairs(clicks: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  val read = KStream(Named("clicks"), clicks, text, text)
  read.into(
    // Join the stream with itself, pairing clicks at most a minute apart
    read.flow.join(Named("pairs"), read.flow, JoinWindows(1.minute, grace = 10.seconds), Store("pairs"), text, text, text)(_ + _)
      .to(Named("write"), output, text, text)
  )
```

As in Java, a stream joined with itself pairs each record with itself too, as well
as with the other records in its window.

A table is sent down several paths in the same way, with its own `flow`, `into` and
`fork`:

```scala mdoc:compile-only
// Join a table with itself
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def selfJoined(users: Topic, output: Topic): Pipeline[IO] =
  val text  = Serde.utf8[IO]
  val table = KTable(Named("users"), users, text, text, Store("users"))
  table.into(
    // Both sides of the join are the same table
    table.flow.join(Named("self"), table.flow)(_ + _).toStream(Named("changes")).to(Named("write"), output, text, text.option)
  )
```

`Topology.of` refuses, with a `TopologyError`, a join reading a table no records
reach. A table read from a topic needs none of this to be read by several joins, or
to be read while its changes are written somewhere: use the one table in each.

## Flows whose records may have a new key

A flow made with `Flow` has no source to say how its records are keyed, so every
path that groups or joins them goes through a repartition topic. `Topology.of`
refuses that topic with `TopologyError.UnusedRepartition` if none of the records
reaching it have a new key. In that case start the flow with `Flow.asRead` or
`s.flow`, which need no repartition topic for those records.

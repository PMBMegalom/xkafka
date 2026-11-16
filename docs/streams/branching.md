# Branching

A stream is a description of steps, so each use of it is another copy of them all.
A stream used twice would read its topic twice, and `Topology.of` refuses that with
a `TopologyError`. To send records down several paths, say so where the stream
branches.

`fork` sends each record down the first path, including everything that follows
on it, then down the next, so a later path sees what an earlier one did to a store.
Only then does the next record start. Each path starts from the stream's records and ends in a topic or a
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
of partitions. Grouping or joining after `merge` needs a repartition topic if
either side may have changed its key:

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

`split` sends each record down the first branch that takes it. `defaultBranch`
takes what no branch took; with `noDefaultBranch`, those records go no further:

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

A branch's step is named by the split's name and the branch's together, such as
`route-large`, and `Topology.of` refuses one Kafka's rules for names refuse, as
it refuses any step's name. Java names the node the same way without checking it.

## Paths that share what follows

With `fork`, each path has steps of its own, so two paths cannot lead into the same
steps. To share steps between paths, write them first as a flow, then send records
to it with `into`.
A flow starts from one of:

- `Flow.asRead[F, K, V]`, for records keyed as they were read;
- `Flow[F, K, V]`, for records whose key may have changed;
- `s.flow`, for the records of the stream `s`, whose types and key it takes on.

`into` sends each record to any number of flows, in the order given, one finishing
before the next starts, as `fork` does. To match a Java program, list the flows in
the order its paths are written in.

Here large orders and every refund go to both of two topics:

```scala mdoc:compile-only
// Send large orders and every refund to both of two topics
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def reviewed(orders: Topic, refunds: Topic, all: Topic, review: Topic) =
  val text     = Serde.utf8[IO]
  // A flow that writes to the topic "all"
  val toAll    = Flow.asRead[IO, String, String].merged(Named("all")).to(Named("write-all"), all, text, text)
  // A flow that writes to the topic "review"
  val toReview = Flow.asRead[IO, String, String].merged(Named("review")).to(Named("write-review"), review, text, text)
  // A flow that keeps large amounts, and sends them to both of the above
  val large    = Flow.asRead[IO, String, String].filter(Named("large"))((_, amount) => amount.length > 3).into(toAll, toReview)
  Topology.of(
    KStream(Named("orders"), orders, text, text).into(large),             // Orders go through the flow that keeps large amounts
    KStream(Named("refunds"), refunds, text, text).into(toAll, toReview)  // Every refund goes straight to the flows for both topics
  )
```

When a flow gets records from several places, they all go straight to its first
step. `merged` adds a step of its own to receive them, as `merge` would. Use it when the
program's description must match a Java program that merges those paths (see
[Mixing with Java Kafka Streams](java-kafka-streams.md)).

## Joining a stream or a table with itself

Flows are also how a stream is joined with something made from itself, such as its
own count, or with itself. `counts.consumer` ends the count's flow without further
steps: the stream sends records into it, and the join reads the count. Here the
count's path comes first, so the join sees a count that includes the record it is
joining:

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

A stream joined with itself pairs each record with itself too, as well
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

If a join reads a table that no records reach, `Topology.of` refuses the program
with `TopologyError.UnfedTable`. A table read from a topic needs none of this to be
read by several joins, or to be read while its changes are written somewhere: use
the one table in each.

## Flows whose records may have a new key

A flow made with `Flow` has no source to say how its records are keyed, so every
path that groups or joins them goes through a repartition topic. `Topology.of`
refuses that topic with `TopologyError.UnusedRepartition` if none of the records
reaching it have a new key. In that case start the flow with `Flow.asRead` or
`s.flow`, which need no repartition topic for those records.

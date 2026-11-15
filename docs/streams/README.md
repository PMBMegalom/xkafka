# Streams

`KafkaClient.streams` runs a Kafka Streams program. Each running instance is one
member of the application, and the application can mix xkafka members with Java
Kafka Streams instances running the same program (see
[Mixing with Java Kafka Streams](java-kafka-streams.md)). Members share the work
and take it over from each other as they start and stop.

Streams applications need brokers of Kafka 4.2 or newer.

```scala mdoc:compile-only
// Count each page's views across all users, and write every new count to a topic
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val user  = Serde.utf8[IO]
val page  = Serde.utf8[IO]
val views = Serde.long[IO]

val program =
  for
    input    <- Topic.from("page-views").liftTo[IO]
    output   <- Topic.from("views-per-page").liftTo[IO]
    pipeline  = KStream(Named("page-views"), input, user, page)
                  // Key each view by its page, through a repartition topic
                  .groupBy(Repartition("views-by-page"), page, page)((_, viewed) => viewed)
                  .count(Named("count-views"), Store("views-per-page"))  // Keep each page's count in a store
                  .toStream(Named("view-counts"))                        // Turn each change to a count into a record
                  .to(Named("write-view-counts"), output, page, views)   // Write each change to the output topic
    topology <- Topology.of(pipeline).liftTo[IO]                         // Check the program for problems
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- ApplicationId.from("page-view-counter").liftTo[IO]
    settings <- StreamsSettings.from(client, id).liftTo[IO]
    _        <- KafkaClient[IO].streams(settings, topology).use(_.run)  // Join the application and keep processing
  yield ()
```

The input topic holds a record for each page a user views, keyed by the user.
The program counts the views of each page, and writes every new count to
`views-per-page`, keyed by the page.

Acquiring the resource joins the application and starts processing. `run` waits
while the application runs, and fails with the error that stops it. Releasing
the resource commits the member's work, hands it back to the application, and
leaves, waiting at most the settings' `closeTimeout`, 30 seconds by default.
[Running a program](running.md) covers guarantees, concurrency and stores.

## Programs

A program is a `Topology` of pipelines. A pipeline starts by reading a topic, as a
`KStream` or as a `KTable` (see [Tables](tables.md)), and is built with:

- `filter`, `filterNot`, `map`, `mapValues`, `flatMap`, `flatMapValues`,
  `selectKey` and `peek`;
- `evalFilter`, `evalMap` and `evalMapValues`, which run an effect;
- `filterRecord`, `mapRecord`, `mapRecordValue` and others like them, which also
  see each record's time and headers (see [Steps that see the whole record](#steps-that-see-the-whole-record));
- `repartition`, which sends records through a repartition topic you name, with as
  many partitions as you choose (see [Choosing partitions](partitioning.md));
- `groupByKey` or `groupBy`, followed by `count`, `reduce` or `aggregate` into a
  named `Store`. This gives a `KTable`, and its `toStream` is every change to it.
  A table can be grouped too (see [Grouping a table](tables.md#grouping-a-table)),
  and several groups can be folded together
  (see [Folding streams together](tables.md#folding-streams-together));
- `toTable`, which keeps each key's latest value as a `KTable`;
- `join`, `leftJoin` and `outerJoin`, which combine it with a table or another
  stream. `join` and `leftJoin` also look up a global table, and a table can join
  another table on a foreign key with `joinOnForeignKey` and
  `leftJoinOnForeignKey` (see [Joins](joins.md));
- `fork`, `forkMerge`, `merge`, `split` and `into`, which send records down
  several paths or bring them together (see [Branching](branching.md));
- `to`, which ends a path at an output topic, or `foreach` and `print`, which end
  it by acting on each record or printing it.

A `KStream` can read several topics as one source: pass them as a `NonEmptyList`.
Every record is read with the same serdes, and each deserializer is told which
topic the record came from. A topic listed twice in that list is read once.
Partitions with the same number are processed together, so if you group or join
the records by key, give the topics the same number of partitions.

Each topic and store is read and written with a `Serde`, which pairs a
[serializer and a deserializer](../serialization.md) of one type, since a streams
application both reads and writes. `Serde(serializer, deserializer)` makes one.
`.option` turns a serde into one of `Option`s, which reads and writes a null as
`None`, so a `None` value is written as a tombstone. `Serde.utf8`, `Serde.long`
and `Serde.bytes` write the same bytes as Java's `Serdes.String()`,
`Serdes.Long()` and `Serdes.ByteArray()`.

Every step has a name, given as a `Named`: a pipeline's source, each step, and
its `to`. Names label the program's steps in
[its description](#describing-a-program). A step that keeps a store also takes the
store's name, as a `Store`. Two steps cannot share a name, and no step can
take a name the program gives a node of its own: the source of the repartition
`orders` is `orders-repartition-source`, and a cogroup named `totals` merges in
`totals-cogroup-merge`. A join of two streams names no node by its own name, so
a step may take that name.

Grouping records whose key may have changed, after `map`, `flatMap`, `selectKey`
or `evalMap`, sends them through a repartition topic first. Name it with a
`Repartition`: `groupBy` always takes one, and `groupByKey` takes one after such
a step.

In the first example, `groupBy` keys each view by its page, so the views of a
page, from every user, are counted together. They go through the repartition
topic `<application id>-views-by-page-repartition`. Choosing the key first
with `selectKey` gives the same counts, with `groupByKey` naming the
repartition:

```scala mdoc:compile-only
// The same counts, choosing the key first and naming the repartition when grouping
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def viewsPerPage(input: Topic, output: Topic): Pipeline[IO] =
  val user  = Serde.utf8[IO]
  val page  = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .selectKey(Named("by-page"))((_, viewed) => viewed)    // Key each view by its page
    .groupByKey(Repartition("views-by-page"), page, page)  // The key changed, so this takes a Repartition
    .count(Named("count-views"), Store("views-per-page"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, page, Serde.long[IO])
```

Without the `Repartition`, this does not compile: the views of a page must
reach one partition before they can be counted together. A stream's type says
whether its records are keyed as they were read, so the compiler knows which
`groupByKey` applies:

| Key after the step | Steps | Group with |
| --- | --- | --- |
| As read | `KStream(...)`, then `filter`, `filterNot`, `mapValues`, `flatMapValues`, `peek`, `evalFilter`, `evalMapValues` | `groupByKey(key, value)` |
| May have changed | `map`, `flatMap`, `selectKey`, `evalMap`, `mapRecord`, `evalMapRecord`, `flatMapRecord` | `groupByKey(repartition, key, value)` |

Each `groupByKey` fails to compile if the other applies, with a message saying
which to use. Joining after a step that may have changed the key also needs a
repartition topic.

`repartition` sends records through a repartition topic whenever you ask, whether
or not their key has changed. Afterwards they are keyed as read from that topic,
so `groupByKey` needs no name for them.

`foreach` runs an effect for each record, and runs it again for records processed
again, such as after a crash, as `peek` does. `print` writes each record as
`[label]: key, value` to the `Console[F]` in scope. The label is the one you give.
Without one, it is the name of the step before it, or `print`'s own name if it
starts a [flow](branching.md#paths-that-share-what-follows).

The application's own topics are named after its id: a store's changelog is
`<application id>-<store>-changelog`, and a repartition topic is
`<application id>-<name>-repartition`. As in Java, a stream's repartition whose
name already ends in `-repartition` takes it as it is, so `orders-repartition`
is `<application id>-orders-repartition`; a grouped table's always has the
suffix added. The broker creates them when the application first starts.

`Topology.of` checks the program and reports every problem at once, such as two
stores or steps with the same name, or one topic read by two sources (to send
one stream down several paths, see [Branching](branching.md)). `liftTo`
raises them in `F` as an `InvalidTopology`, and refused settings as an
`InvalidStreamsSettings`.

### Steps that see the whole record

`filter`, `mapValues` and the steps like them see a record's key and value. To read
or change its headers or time, or see where it was read from, use a step that is
given the whole record:

```scala mdoc:compile-only
// Drop test orders, and put the tenant named in a header into each order's value
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def tenantOrders(orders: Topic, output: Topic): Pipeline[IO] =
  val id   = Serde.utf8[IO]
  val item = Serde.utf8[IO]
  KStream(Named("orders"), orders, id, item)
    .filterRecord(Named("real-orders"))(_.headers.getAll("x-test").isEmpty)  // Test orders carry an x-test header
    .mapRecordValue(Named("with-tenant")): record =>
      val tenant = record.headers.get[String]("tenant").flatMap(_.toOption).getOrElse("unknown")  // Read the tenant header, or use unknown if it is missing or unreadable
      record.withValue(s"$tenant: ${record.value}").withHeaders(record.headers.remove("tenant"))  // Change the value and headers
    .to(Named("write-orders"), output, id, item)
```

| To | Use | With an effect |
| --- | --- | --- |
| Keep some records | `filterRecord` | `evalFilterRecord` |
| Act on each record and leave it as it is | | `peekRecord` |
| Change the value, time or headers | `mapRecordValue` | `evalMapRecordValue` |
| Change those or the key | `mapRecord` | `evalMapRecord` |
| Make any number of records, keeping the key | `flatMapRecordValue` | |
| Make any number of records, with any key | `flatMapRecord` | |

Each is given a `Record`, or a `FixedKeyRecord` if the key cannot change, as a
[processor](processors.md) is. `withValue` and `withKey` keep the record's time and
headers, and `withTimestamp` and `withHeaders` change them. `record.origin` gives the
topic, partition and offset the record was read from. See
[Headers](../headers.md) for reading and changing headers.

Grouping or joining after `mapRecord`, `evalMapRecord` or `flatMapRecord` needs a
repartition topic, as it does after `map`. The other steps keep the key.

To make any number of records with an effect, or to use a store, write a
[processor](processors.md).

### Writing a stream's type

You need this only to annotate a stream's type. The stream's `Keying` type member
says how its records are keyed: `KeyState.AsRead` or `KeyState.Changed`. Types are
inferred, so a program rarely names it. To annotate a stream without losing it, use
`KStream.Of`, as in `KStream.Of[IO, String, String, KeyState.AsRead]`.

A `KStream.Unkeyed[IO, String, String]` is a stream whose type does not say how it
is keyed, and it is taken to have changed its key. Grouping it by key takes a
`Repartition`, and if its key had not changed after all, `Topology.of` refuses the
name with `TopologyError.UnusedRepartition`.

### Describing a program

`describe` shows what a program will run: the topics it reads and writes, the
order of its steps, the stores each step uses, and where a repartition topic
divides it. It needs no broker, so a program can be printed or checked in a test before
it runs. Here is the first example's:

```scala mdoc:silent
// Build the first example's program, and describe it
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val described =
  for
    input    <- Topic.from("page-views").toOption
    output   <- Topic.from("views-per-page").toOption
    topology <- Topology.of(
                  KStream(Named("page-views"), input, Serde.utf8[IO], Serde.utf8[IO])
                    .groupBy(Repartition("views-by-page"), Serde.utf8[IO], Serde.utf8[IO])((_, viewed) => viewed)
                    .count(Named("count-views"), Store("views-per-page"))
                    .toStream(Named("view-counts"))
                    .to(Named("write-view-counts"), output, Serde.utf8[IO], Serde.long[IO])
                ).toOption
  yield topology.describe  // Describing a program needs no broker
```

```scala mdoc
// Print the description
described.foreach(println)
```

Each sub-topology is a part of the program that runs as tasks of its own, and a
repartition topic joins two of them: here the first writes
`views-by-page-repartition` and the second reads it. `-->` points to the nodes a
node passes its records to, and `<--` to the nodes it gets them from.

Each step appears under its name. A repartition topic adds the nodes that write
and read it, named after it. The topic is shown without the application's id,
so on the broker this one is `<application id>-views-by-page-repartition`.

Java Kafka Streams' `Topology.describe` writes the same text for the same
program, which lets you compare the two when moving a program between them (see
[Mixing with Java Kafka Streams](java-kafka-streams.md)).

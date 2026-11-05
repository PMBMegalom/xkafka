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

val user  = Serde.utf8[IO]
val page  = Serde.utf8[IO]
val views = Serde.long[IO]

val program =
  for
    input    <- Topic.from("page-views").liftTo[IO]
    output   <- Topic.from("views-per-page").liftTo[IO]
    pipeline  = KStream(Named("page-views"), input, user, page)
                  .groupBy(Repartition("views-by-page"), page, page)((_, viewed) => viewed)
                  .count(Named("count-views"), Store("views-per-page"))
                  .toStream(Named("view-counts"))
                  .to(Named("write-view-counts"), output, page, views)
    topology <- Topology.of(pipeline).liftTo[IO]
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- ApplicationId.from("page-view-counter").liftTo[IO]
    settings <- StreamsSettings.from(client, id).liftTo[IO]
    _        <- KafkaClient[IO].streams(settings, topology).use(_.run)
  yield ()
```

The input topic holds a record for each page a user views, keyed by the user.
The program counts the views of each page, and writes every new count to
`views-per-page`, keyed by the page.

Acquiring the resource joins the application and starts processing. `run` waits
while the application runs, and fails with the error that stops it. Releasing
the resource commits the member's work, hands it back to the application, and
leaves, waiting at most `closeTimeout`.

## Programs

A program is a `Topology` of pipelines. A pipeline starts by reading a topic, as a
`KStream` or as a `KTable` (see [Tables](#tables)), and is built with:

- `filter`, `filterNot`, `map`, `mapValues`, `flatMap`, `flatMapValues`,
  `selectKey` and `peek`;
- `evalFilter`, `evalMap` and `evalMapValues`, which run an effect;
- `repartition`, which sends records through a repartition topic you name;
- `groupByKey` or `groupBy`, followed by `count`, `reduce` or `aggregate` into a
  named `Store`. This gives a `KTable`, and its `toStream` is every change to it;
- `toTable`, which keeps each key's latest value as a `KTable`;
- `join`, `leftJoin` and `outerJoin`, which combine it with a table or a stream
  (see [Joins](#joins));
- `fork`, `forkMerge`, `merge`, `split` and `into`, which send records down
  several paths or bring them together (see [Branching](#branching));
- `to`, which ends a path at an output topic, or `foreach` and `print`, which end
  it by acting on each record or printing it.

A `KStream` can read several topics as one source, given as a `NonEmptyList`, as
Java's `stream` can given several topics. A record is read with the same serdes
whichever topic it came from, and each deserializer is told that topic. A topic
listed twice is read once. A task reads one partition number of each topic, so to
keep a key's records together for grouping or joining, give the topics the same
number of partitions.

Every step has a name, given as a `Named`: a pipeline's source, each step, and
its `to`. Names label the program's nodes in [its description](#describing-a-program), as
`Named`, `Consumed` and `Produced` names do in Java Kafka Streams. A step that keeps a store also takes the store's name, as
Java takes a `Named` and a `Materialized` name. Two steps cannot share a name.

Grouping records whose key may have changed, after `map`, `flatMap`, `selectKey`
or `evalMap`, sends them through a repartition topic first. Name it with a
`Repartition`: `groupBy` always takes one, and `groupByKey` takes one after such
a step. As with Java's `Grouped`, the step that gives `groupBy`'s records their
new key takes the repartition's name.

In the first example, `groupBy` keys each view by its page, so the views of a
page, from every user, are counted together. They go through the repartition
topic `<application id>-views-by-page-repartition`. Choosing the key first
with `selectKey` gives the same counts, with `groupByKey` naming the
repartition:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def viewsPerPage(input: Topic, output: Topic): Pipeline[IO] =
  val user  = Serde.utf8[IO]
  val page  = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .selectKey(Named("by-page"))((_, viewed) => viewed)
    .groupByKey(Repartition("views-by-page"), page, page)
    .count(Named("count-views"), Store("views-per-page"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, page, Serde.long[IO])
```

Without the `Repartition`, this does not compile: the views of a page must
reach one partition before they can be counted together. A stream's type says
whether its records are keyed as they were read, so the compiler knows which
`groupByKey` applies. `KStream(...)` and `filter`, `filterNot`, `mapValues`,
`flatMapValues`, `peek`, `evalFilter` and `evalMapValues` after it keep the key as read, and
`groupByKey(key, value)` groups the records where they are. After `map`,
`flatMap`, `selectKey` or `evalMap`, only `groupByKey(repartition, key, value)`
compiles. Each fails to compile where the other applies, with a message saying
which to use.

The stream's `Keying` type member carries this: `KeyState.AsRead` or
`KeyState.Changed`. Types are inferred, so a program rarely names it. To
annotate a stream without losing it, use `KStream.Of`, as in
`KStream.Of[IO, String, String, KeyState.AsRead]`. A
`KStream.Unkeyed[IO, String, String]` is a stream read from a topic whose type does
not say how it is keyed, and it is taken to have changed its key: grouping it by
key takes a `Repartition`, and if its key had not changed after all, `Topology.of`
refuses the name with `TopologyError.UnusedRepartition`.

`repartition` sends records through a repartition topic whenever you ask, whether
or not their key has changed. Afterwards they are keyed as read from that topic,
so `groupByKey` needs no name for them.

`foreach` runs an effect for each record, and runs it again for records replayed
after a crash or a fencing, as `peek` does. `print` writes each record as
`[label]: key, value` to the `Console[F]` in scope, labelled with the name of the
step before it unless you give a label, or with its own name if it starts a
[flow](#paths-that-share-what-follows). Java's `print` writes the same lines.

The application's own topics are named after its id: a store's changelog is
`<application id>-<store>-changelog`, and a repartition topic is
`<application id>-<name>-repartition`. The broker creates them when the
application first starts.

`Topology.of` checks the program and reports every problem at once, such as two
stores or steps with the same name, or one topic read twice. `liftTo`
raises them in `F` as an `InvalidTopology`, and refused settings as an
`InvalidStreamsSettings`.

A `Serde` holds the serializer and deserializer of one type, since a streams
application both reads and writes its topics. `Serde.utf8`, `Serde.long` and
`Serde.bytes` write the same bytes as Java's `Serdes.String()`, `Serdes.Long()`
and `Serdes.ByteArray()`.

### Describing a program

`describe` shows what a program will run: the topics it reads and writes, the
order of its steps, the stores each step uses, and where a repartition divides
it. It needs no broker, so a program can be printed or checked in a test before
it runs. Here is the first example's:

```scala mdoc:silent
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
  yield topology.describe
```

```scala mdoc
described.foreach(println)
```

Each sub-topology is a part of the program that runs as tasks of its own, and a
repartition topic joins two of them: here the first writes
`views-by-page-repartition` and the second reads it. `-->` points to the nodes a
node passes its records to, and `<--` to the nodes it gets them from.

Each step appears under its name. A repartition adds the nodes that write and
read its topic, named after it. The topic is shown without the application's id,
so on the broker this one is `<application id>-views-by-page-repartition`.

Java Kafka Streams' `Topology.describe` writes the same text for the same
program, which lets you compare the two when moving a program between them.

## Tables

A `KTable` holds the latest value of each key. Besides the tables that `count`,
`reduce` and `aggregate` make, a table can be read straight from a topic, as Java
Kafka Streams' `StreamsBuilder.table` reads it:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def userNames(users: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val name = Serde.utf8[IO]
  KTable(Named("users"), users, user, name, Store("users"))
    .toStream(Named("user-changes"))
    .to(Named("write-user-names"), output, user, name.option)
```

Each record's value replaces its key's, even a record timestamped earlier than
the one it replaces. A record without a value (a tombstone) deletes its key, and
a record without a key is skipped. The table is kept in the named store, whose
changelog is `<application id>-<store>-changelog`, here
`<application id>-users-changelog`.

A table read from a topic can delete keys, so its `toStream` gives `Option`
values: `Some` for an update and `None` for a delete, which `Serde.option`
writes as a tombstone, as Java writes its null. A table that only ever updates
its keys, such as a `count`, gives its values as they are. The table's `Change`
type member says which, and `KTable.Of` names it in an annotation:
`KTable.Of[F, K, V, Option]` can delete, and `KTable.Of[F, K, V, KTable.Updated]`
only updates. Writing a table that can delete without `.option` does not
compile. A plain `KTable[F, K, V]` does not say, so its `toStream` cannot be
written with either serde.

A stream becomes a table with `toTable`, which keeps each key's latest value in a
store. Records whose key may have changed go through the repartition topic
`<application id>-<step>-repartition` first, so the step's name also names that
topic. For a stream of `Option` values, passing the serde of the values inside
makes none delete the key:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def latestAddress(moves: Topic, output: Topic): Pipeline[IO] =
  val user    = Serde.utf8[IO]
  val address = Serde.utf8[IO]
  KStream(Named("moves"), moves, user, address.option)
    .toTable(Named("latest-address"), Store("addresses"), user, address)
    .toStream(Named("address-changes"))
    .to(Named("write-address-changes"), output, user, address.option)
```

### Filtering and changing a table

A table's `filter`, `filterNot` and `mapValues` give another table, which follows
every change to the first. `filter` and `filterNot` turn a value they drop into a
delete, so the result can always delete keys, and its changes are `Option`s. A
key that had no value in the result and still has none passes nothing on.
`mapValues` passes a delete on as a delete:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def activeEmails(users: Topic, orders: Topic, output: Topic): Pipeline[IO] =
  val id   = Serde.utf8[IO]
  val text = Serde.utf8[IO]
  val emails =
    KTable(Named("users"), users, id, text, Store("users"))
      .filterNot(Named("not-closed"))((_, user) => user.startsWith("closed:"))
      .mapValues(Named("email"))(_.split(' ').last)
  KStream(Named("orders"), orders, id, text)
    .join(Named("order-email"), emails)((order, email) => s"$order for $email")
    .to(Named("write-order-emails"), output, id, text)
```

These tables keep no store of their own: a join reads through to the store behind
them, applying the filter and the change as it reads. To keep the result, which
adds a changelog topic, give a `Store` and a `Serde`, as in
`filter(Named("kept"), Store("kept"), text)(keep)`. `suppress` needs a serde for
the values it holds, so a table from `mapValues` must be kept in a store before it
is suppressed; `Topology.of` refuses it otherwise.

## Joins

`join` pairs each record of a stream with its key's value in a table, as Java
Kafka Streams' stream-table join does. Here each order, keyed by the user who
placed it, is joined with that user's name:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithNames(orders: Topic, users: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val item = Serde.utf8[IO]
  val name = Serde.utf8[IO]
  KStream(Named("orders"), orders, user, item)
    .join(Named("with-name"), KTable(Named("users"), users, user, name, Store("users")))((bought, buyer) => s"$bought for $buyer")
    .to(Named("write-orders"), output, user, Serde.utf8[IO])
```

Each record is joined with the value the table holds for its key when the record
is processed; later changes to the table do not change records already joined.
A record whose key the table holds no value for, never set or deleted, is
dropped. `leftJoin` keeps it, passing `None` instead of the table's value. The
result keeps each record's timestamp and headers.

The table can be read from a topic, made by `count`, `reduce`, `aggregate` or
`toTable`, or come from another table's `filter`, `mapValues` or join. Joining a suppressed table sees, for a key the
suppression holds, the value from before the change it holds, as Java does.

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

The description also numbers the application's sub-topologies. When an input of
a join has sub-topologies of its own, for example a table counted after a
repartition, Java numbers them by which input the Java program builds first,
which xkafka cannot see. xkafka numbers them as a program does that builds the
join's receiver first, before anything of the other input, and lists
`StreamsProblem.JavaMembersMayBeRefused` for each such join. To keep the Java
members, build the receiver first in the Java program too:

```java
KStream<String, String> orders = builder.stream("orders");
KTable<String, Long> visits = builder.<String, String>stream("visits")
    .groupBy((key, page) -> page, Grouped.as("by-page"))
    .count(Materialized.as("visits"));
orders.join(visits, (order, count) -> order + count).to("out");
```

If the stream's key may have changed since it was read, for example after
`selectKey`, its records first go through a repartition topic, and `join` and
`leftJoin` compile only with serdes to write it with, as `groupByKey` compiles
only with a name. The topic is named after the join,
`<application id>-<name>-repartition`, as Java names it after its `Joined` name:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersByUser(orders: Topic, users: KTable.Of[IO, String, String, Option], output: Topic): Pipeline[IO] =
  val id   = Serde.utf8[IO]
  val item = Serde.utf8[IO]
  KStream(Named("orders"), orders, id, item)
    .selectKey(Named("by-user"))((order, _) => order.takeWhile(_ != '/'))
    .join(Named("orders-by-user"), users, id, item)((bought, buyer) => s"$bought for $buyer")
    .to(Named("write-orders"), output, id, Serde.utf8[IO])
```

Here the orders are keyed by an order id such as `alice/1042`, and `selectKey`
keys them by the user before the join.

### Joining tables

Two tables with the same key join into a table, as Java Kafka Streams' table-table
joins do. Every change to either table updates the result for its key:

```scala mdoc:compile-only
import cats.data.Ior
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def accounts(users: Topic, plans: Topic, output: Topic): Pipeline[IO] =
  val user  = Serde.utf8[IO]
  val name  = Serde.utf8[IO]
  val plan  = Serde.utf8[IO]
  val names = KTable(Named("users"), users, user, name, Store("users"))
  val paid  = KTable(Named("plans"), plans, user, plan, Store("plans"))
  names
    .outerJoin(Named("accounts"), paid) {
      case Ior.Left(who)       => s"$who, no plan"
      case Ior.Right(tier)     => s"unknown user on $tier"
      case Ior.Both(who, tier) => s"$who on $tier"
    }
    .toStream(Named("account-changes"))
    .to(Named("write-accounts"), output, user, Serde.utf8[IO].option)
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
names
  .join(Named("accounts"), paid, Store("accounts"), Serde.utf8[IO])((who, tier) => s"$who on $tier")
  .suppress(Suppressed.untilTimeLimit("accounts-calm", 1.second))
```

Like a stream and a table, two joined tables are processed together, so their
topics must have the same number of partitions. To join a table with itself, see
[Branching](#branching).

### Joining streams

Two streams with the same key join within windows of time, as Java Kafka
Streams' stream-stream joins do. Each record joins every record of the other
stream with its key whose time is close enough to its own. Here orders and
payments are both keyed by the order, and each order should be paid within
five minutes:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.data.Ior
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def payments(orders: Topic, payments: Topic, output: Topic): Pipeline[IO] =
  val order = Serde.utf8[IO]
  val item  = Serde.utf8[IO]
  val cents = Serde.long[IO]
  KStream(Named("orders"), orders, order, item)
    .outerJoin(
      Named("pay"),
      KStream(Named("payments"), payments, order, cents),
      JoinWindows(5.minutes, grace = 30.seconds),
      Store("payments"),
      order,
      item,
      cents
    ) {
      case Ior.Left(bought)       => s"$bought, unpaid"
      case Ior.Right(paid)        => s"$paid cents paid for no order"
      case Ior.Both(bought, paid) => s"$bought, paid $paid cents"
    }
    .to(Named("write-payments"), output, order, Serde.utf8[IO])
```

`JoinWindows(5.minutes, grace = 30.seconds)` joins records at most five minutes
apart, either way. `withBefore` and `withAfter` set how far back and how far
ahead of a record the other stream's records may be, so the windows can reach
further one way than the other. A record that arrives more than the grace after
the latest time either stream has read may find that the other stream's records
in its window have already been dropped.

- `join` writes a result for each pair of records in a window.
- `leftJoin` also writes each record of the left stream that matched nothing,
  passing the right stream's value as `None`.
- `outerJoin` also writes each record of either stream that matched nothing,
  passing the values as an `Ior`.

Each pair takes the later timestamp of its two records. A record that matched
nothing is held until its window and grace have passed, in case a match
arrives, and is then written alone with its own timestamp. As in Java, xkafka
looks for such records when a record arrives, and at most once a second.

Each stream's records are kept in a store for the windows' size and grace, and
a left or outer join keeps the records that matched nothing in a third store.
The stores are named from the store's name, as Java names them:

| Join        | Stores                                                                                                  |
|-------------|---------------------------------------------------------------------------------------------------------|
| `join`      | `<name>-this-join-store`, `<name>-other-join-store`                                                     |
| `leftJoin`  | `<name>-this-join-store`, `<name>-outer-other-join-store`, `<name>-left-shared-join-store`              |
| `outerJoin` | `<name>-outer-this-join-store`, `<name>-outer-other-join-store`, `<name>-outer-shared-join-store`       |

The three serdes write the key and each stream's values to the stores. A
stream whose key has changed is written first to a repartition topic named
after the join, `<application id>-<join>-left-repartition` or
`<application id>-<join>-right-repartition`, with the same serdes. A Java
program names everything the same way with
`StreamJoined.with(keySerde, valueSerde, otherValueSerde).withName(join).withStoreName(store)`.

A record without a key, one whose key serde writes none, is written alone at
once by a left join's left stream and by either stream of an outer join, and
is otherwise dropped.

The two streams are processed together, so their topics must have the same
number of partitions. To join a stream with itself, see [Branching](#branching).

## Branching

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
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def audited(orders: Topic, all: Topic, audit: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text).fork(
    _.to(Named("all"), all, text, text),
    _.mapValues(Named("mark"))(order => s"audit: $order").to(Named("audit"), audit, text, text)
  )
```

`forkMerge` works like `fork`, and what the paths make goes on as one stream.

`merge` makes one stream of two, in one sub-topology, and their topics need not
have the same number of partitions. A merge is keyed as read only if each of its
sides is:

```scala mdoc:compile-only
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def movements(orders: Topic, refunds: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text)
    .merge(Named("movements"), KStream(Named("refunds"), refunds, text, text))
    .forkMerge(Named("marked"))(
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
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def routed(orders: Topic, large: Topic, small: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  KStream(Named("orders"), orders, text, text)
    .split(Named("route-"))
    .branch(Named("large"))((_, amount) => amount.length > 3)(_.to(Named("write-large"), large, text, text))
    .defaultBranch(Named("small"))(_.to(Named("write-small"), small, text, text))
```

### Paths that share what follows

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
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def reviewed(orders: Topic, refunds: Topic, all: Topic, review: Topic) =
  val text     = Serde.utf8[IO]
  val toAll    = Flow.asRead[IO, String, String].merged(Named("all")).to(Named("write-all"), all, text, text)
  val toReview = Flow.asRead[IO, String, String].merged(Named("review")).to(Named("write-review"), review, text, text)
  val large    = Flow.asRead[IO, String, String].filter(Named("large"))((_, amount) => amount.length > 3).into(toAll, toReview)
  Topology.of(
    KStream(Named("orders"), orders, text, text).into(large),
    KStream(Named("refunds"), refunds, text, text).into(toAll, toReview)
  )
```

A flow sent records from several places has its first step read from all of them.
`merged` gives it a node of its own to do that, as `merge` would.

### Joining a stream or a table with itself

Flows are also how a stream is joined with something made from itself, such as its
own count, or with itself. `consumer` ends a table with no further steps, so that
it can be sent to while a join reads it. Here the count's path comes first, so the
join sees a count that includes the record it is joining:

```scala mdoc:compile-only
import scala.concurrent.duration.*
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def withCounts(clicks: Topic, output: Topic): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val read   = KStream(Named("clicks"), clicks, text, text)
  val counts = read.flow.groupByKey(text, text).count(Named("counted"), Store("counts"))
  read.into(
    counts.consumer,
    read.flow.join(Named("with-count"), counts)((click, seen) => s"$click, seen $seen").to(Named("write"), output, text, text)
  )

def pairs(clicks: Topic, output: Topic): Pipeline[IO] =
  val text = Serde.utf8[IO]
  val read = KStream(Named("clicks"), clicks, text, text)
  read.into(
    read.flow.join(Named("pairs"), read.flow, JoinWindows(1.minute, grace = 10.seconds), Store("pairs"), text, text, text)(_ + _)
      .to(Named("write"), output, text, text)
  )
```

As in Java, a stream joined with itself pairs each record with itself too, as well
as with the other records in its window.

A table is sent down several paths in the same way, with its own `flow`, `into` and
`fork`:

```scala mdoc:compile-only
import cats.effect.IO
import xkafka.*
import xkafka.streams.*

def selfJoined(users: Topic, output: Topic): Pipeline[IO] =
  val text  = Serde.utf8[IO]
  val table = KTable(Named("users"), users, text, text, Store("users"))
  table.into(
    table.flow.join(Named("self"), table.flow)(_ + _).toStream(Named("changes")).to(Named("write"), output, text, text.option)
  )
```

`Topology.of` refuses, with a `TopologyError`, a join reading a table no records
reach. A table read from a topic needs none of this to be read by several joins, or
to be read while its changes are written somewhere: use the one table in each.

### Flows whose records may have a new key

A flow made with `Flow` has no source to say how its records are keyed, so every
path that groups or joins them goes through a repartition topic. `Topology.of`
refuses that topic with `TopologyError.UnusedRepartition` if none of the records
reaching it have a new key. In that case start the flow with `Flow.asRead` or
`s.flow`, which need no repartition topic for those records.

## Windows

Windows split each key's records by time, so you can count, reduce or aggregate
them per minute, per hour, and so on. Group the records, then window them:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def viewsPerMinute(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .count(Named("count-views"), Store("views-per-minute"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, Serde.timeWindowed(user, 1.minute), Serde.long[IO])
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

### Final results only

By default, every update to a window goes downstream. To send only each
window's final result, once the window has closed, add `emitOnWindowClose`:

```scala mdoc:compile-only
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
    .withRetention(10.minutes)
    .emitOnWindowClose
    .count(Named("count-views"), Store("final-views-per-minute"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, Serde.timeWindowed(user, 1.minute), Serde.long[IO])
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

Sliding windows answer questions like "how many times did this user fail to
log in within five minutes?". Each window covers records at most a time difference
apart. Windows are made only where records are, so there are no empty
windows:

```scala mdoc:compile-only
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def failedLogins(input: Topic, output: Topic): Pipeline[IO] =
  val user    = Serde.utf8[IO]
  val address = Serde.utf8[IO]
  KStream(Named("failed-logins"), input, user, address)
    .groupByKey(user, address)
    .windowedBy(SlidingWindows(5.minutes, grace = 30.seconds))
    .count(Named("count-failures"), Store("failures-within-five-minutes"))
    .toStream(Named("failure-counts"))
    .to(Named("write-failure-counts"), output, Serde.timeWindowed(user, 5.minutes), Serde.long[IO])
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
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(SessionWindows(5.minutes, grace = 1.minute))
    .count(Named("count-views"), Store("views-per-visit"))
    .toStream(Named("view-counts"))
    .to(Named("write-view-counts"), output, Serde.sessionWindowed(user), Serde.long[IO].option)
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
for it as well, given the two sessions' values. Over a stream of purchases in
cents, keyed by user, this adds up what each user spends per visit:

```scala
.aggregate(Named("add-spending"), Store("spending-per-visit"), Serde.long[IO])(0L)((_, cents, total) => total + cents)((_, first, second) => first + second)
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

def finalViews(input: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val page = Serde.utf8[IO]
  KStream(Named("page-views"), input, user, page)
    .groupByKey(user, page)
    .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
    .count(Named("count-views"), Store("views-per-minute"))
    .suppress(Suppressed.untilWindowCloses("final-views"))
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
.count(Named("count-views"), Store("views-per-page"))
.suppress(Suppressed.untilTimeLimit("calm-views", 1.minute))
```

Once a key has an update, it is held. Later updates replace it. The latest goes
on once a record arrives timestamped a minute or more after the first. Time
here is the records' time, so while no records arrive, nothing goes on.

### Buffer limits

By default a suppression holds as much as it needs to. To limit it, pass a
`BufferLimit`:

```scala
Suppressed.untilTimeLimit("calm-views", 1.minute, BufferLimit.records(10000))
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
topology. A member sends the program's description when asked, as Java Kafka
Streams does, and the broker keeps it for whoever describes the group, such as
Java's `Admin.describeStreamsGroups` with `includeTopologyDescription`.
`StreamsSettings`' `withoutTopologyDescriptionPush` stops members sending it:
the application keeps working, and the group's description has none.

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

# Joins

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
`toTable`, or come from another table's `filter`, `mapValues` or join. A join with a
suppressed table sees, for a key whose update is held back, the value from before
that update, as in Java.

A stream and the table it joins are processed together, so their topics must
have the same number of partitions, as in Java. If they do not, the broker
refuses the application's topics, which shows up in `state` as
`StreamsProblem.IncorrectlyPartitionedTopics`. A member processes the records
it has read from both topics earliest timestamp first, as Java does.

An application whose members all run xkafka can run any join. If some members run
Java Kafka Streams, the broker requires every member to describe the application
in the same way, and may refuse the Java members otherwise. For some pairs of
joined topic names, xkafka cannot tell whether its description will match Java's.
The application's `state` then lists `StreamsProblem.JavaMembersMayBeRefused`,
naming the topics. Joining topics with other names avoids it.

`state` lists the same problem for a join with an input that has sub-topologies of
its own, for example a table counted after a repartition. To keep the Java members,
build what `join` is called on first in the Java program, before anything of the
other input:

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

## Joining tables

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
[Joining a stream or a table with itself](branching.md#joining-a-stream-or-a-table-with-itself).

## Joining streams

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
arrives, and is then written alone with its own timestamp. Such records are
written when a later record arrives, at most once a second, so while no records
arrive they wait.

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
number of partitions. To join a stream with itself, see
[Joining a stream or a table with itself](branching.md#joining-a-stream-or-a-table-with-itself).

## Joining a global table

A `GlobalKTable` holds the latest value of each key of a topic, and every member
of the application keeps all of it, as with Java Kafka Streams' `GlobalKTable`. It
suits reference data small enough for each member to hold. A stream joins it by a
key taken from each record, so the two need not share a key or a number of
partitions. Here each order is joined with the product named in its value:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithProducts(orders: Topic, products: Topic, output: Topic): Pipeline[IO] =
  val text    = Serde.utf8[IO]
  val catalog = GlobalKTable(Named("products"), products, text, text, Store("products"))
  KStream(Named("orders"), orders, text, text)
    .join(Named("with-product"), catalog)((_, product) => product)((product, details) => s"$product: $details")
    .to(Named("write-orders"), output, text, text)
```

The first function gives the table's key for each record, and the second makes the
result. `join` drops a record whose key the table holds no value for, and `leftJoin`
keeps it, passing `None`. The stream keeps its own key through the join, so what
follows needs no repartition topic on account of it. Later changes to the table do
not change records already joined.

A member reads the table's topic to its end as it starts, before it joins the
application, so acquiring the resource waits for that, and fails if the topic does
not exist. It then follows the topic for as long as it runs. If it cannot read a
record it finds there, `run` fails. The table's topic is all that backs it, so it has
no changelog. `Topology.of` refuses a program that also reads that topic elsewhere.

When some members run Java Kafka Streams, the Java program must build the table
after the stream that first joins it, or the broker may refuse the Java members. The
application's `state` lists `StreamsProblem.JavaMembersMayBeRefused` as a reminder:

```java
KStream<String, String> orders = builder.stream("orders");
GlobalKTable<String, String> catalog = builder.globalTable("products");
orders.join(catalog, (order, product) -> product, (product, details) -> product + ": " + details).to("out");
```

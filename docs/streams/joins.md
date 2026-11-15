# Joins

`join` pairs each record of a stream with its key's value in a table. Here each order, keyed by the user who
placed it, is joined with that user's name:

```scala mdoc:compile-only
// Join each order with the name of the user who placed it
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithNames(orders: Topic, users: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val item = Serde.utf8[IO]
  val name = Serde.utf8[IO]
  KStream(Named("orders"), orders, user, item)
    // Look up each order's key in the table
    .join(Named("with-name"), KTable(Named("users"), users, user, name, Store("users")))((bought, buyer) => s"$bought for $buyer")
    .to(Named("write-orders"), output, user, Serde.utf8[IO])
```

Each record is joined with the value the table holds for its key when the record
is processed; later changes to the table do not change records already joined.
A record whose key the table holds no value for, never set or deleted, is
dropped. `leftJoin` keeps it, passing `None` instead of the table's value. The
result keeps each record's timestamp and headers.

The table can be read from a topic, made by `count`, `reduce`, `aggregate` or
`toTable`, or come from another table's `filter`, `mapValues` or join. If the
table holds back updates with `suppress`, the join sees each key's value from
before any update still held back.

A stream and the table it joins are processed together, so their topics must
have the same number of partitions. If they do not, the broker
refuses the application's topics, which shows up in `state` as
`StreamsProblem.IncorrectlyPartitionedTopics`. A member processes the records
it has read from both topics earliest timestamp first.

An application whose members all run xkafka can run any join. If some members run
Java Kafka Streams, some joins need the Java program built in a certain order (see
[Mixing with Java Kafka Streams](java-kafka-streams.md#joins)).

If the stream's key may have changed since it was read, for example after
`selectKey`, its records first go through a repartition topic, and `join` and
`leftJoin` compile only with serdes to write it with, as `groupByKey` compiles
only with a name. The topic is named after the join,
`<application id>-<name>-repartition`:

```scala mdoc:compile-only
// Key orders by user, then join them with the users table
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersByUser(orders: Topic, users: KTable.Of[IO, String, String, Option], output: Topic): Pipeline[IO] =
  val id   = Serde.utf8[IO]
  val item = Serde.utf8[IO]
  KStream(Named("orders"), orders, id, item)
    .selectKey(Named("by-user"))((order, _) => order.takeWhile(_ != '/'))  // The key changes here
    // So the join takes serdes to write a repartition topic
    .join(Named("orders-by-user"), users, id, item)((bought, buyer) => s"$bought for $buyer")
    .to(Named("write-orders"), output, id, Serde.utf8[IO])
```

Here the orders are keyed by an order id such as `alice/1042`, and `selectKey`
keys them by the user before the join.

## Joining tables

Two tables with the same key join into a table. Every change to either table updates the result for its key:

```scala mdoc:compile-only
// Join users and plans into one account per user, whichever of the two a user has
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
    .outerJoin(Named("accounts"), paid) {                   // Every change to either table updates the result
      case Ior.Left(who)       => s"$who, no plan"          // A user with no plan
      case Ior.Right(tier)     => s"unknown user on $tier"  // A plan with no user
      case Ior.Both(who, tier) => s"$who on $tier"          // A user with a plan
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
`Option` values, and `toStream` writes a delete as a tombstone. A delete on
either side of `join` deletes the key from the result, even a key the
result never held. The joining function cannot delete a key itself. Each
result takes the later timestamp of the two values it was made from.

The result is computed when needed and kept in no store. To keep it in one, pass
the store's name and a serde for its values; a table that holds the result back
with `suppress` needs this:

```scala
// Keep the joined table in a store, so that its updates can be held back
names
  // Keep the result in the store named accounts
  .join(Named("accounts"), paid, Store("accounts"), Serde.utf8[IO])((who, tier) => s"$who on $tier")
  .suppress(Suppressed.untilTimeLimit("accounts-calm", 1.second))  // Holding back updates needs the result kept in a store
```

Like a stream and a table, two joined tables are processed together, so their
topics must have the same number of partitions. To join a table with itself, see
[Joining a stream or a table with itself](branching.md#joining-a-stream-or-a-table-with-itself).

## Joining on a foreign key

The tables above share a key. Often a row instead names a row of another table in
its value: an order names its customer. `joinOnForeignKey` joins them, and keeps the
result up to date when either table changes. The tables can have different keys and
different numbers of partitions.

```scala mdoc:compile-only
// Keep each order joined with the name of the customer it belongs to, updated whenever the order or the customer changes.
// Orders are keyed by order id, with values such as "c42:3 books": the customer's id, a colon, then what was ordered.
// Customers are keyed by customer id, with the customer's name as the value.
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithCustomers(orders: Topic, customers: Topic, output: Topic): Pipeline[IO] =
  val text          = Serde.utf8[IO]
  val customerNames = KTable(Named("customers"), customers, text, text, Store("customers"))
  KTable(Named("orders"), orders, text, text, Store("orders"))
    // Name the join, then give the serdes for the orders' values, the customers' keys and the customers' values
    .joinOnForeignKey(Named("order-customer"), customerNames, text, text, text)(
      (_, order) => Option.when(order.contains(':'))(order.takeWhile(_ != ':'))  // Find the customer's id, or none if there is no colon
    )((order, name) => s"$order for $name")                                      // Make the result, kept under the order's id
    .toStream(Named("order-changes"))
    .to(Named("write-orders"), output, text, text.option)                       // Write each change; a deleted result becomes a tombstone
```

The result is keyed as the table `joinOnForeignKey` is called on, here by order id,
and changes in these cases:

| What happens | `joinOnForeignKey` | `leftJoinOnForeignKey` |
| --- | --- | --- |
| An order names a customer that exists | The order gets a result | The order gets a result |
| An order names no customer, or one that does not exist | The order has no result, and loses any it had | The order gets a result, with `None` for the customer |
| A customer changes | Each order naming the customer gets a new result | The same |
| A customer is deleted | Each order naming the customer loses its result | Each order naming the customer gets a result with `None` |
| An order is deleted | Its result is deleted | Its result is deleted |

When an order changes, its result takes the later timestamp of the order and the
customer. When a customer changes, the results take the time of that change. If an
order changes again before the answer for its previous value arrives, that answer
is dropped.

The result is kept in no store. To group it, or to join it with something else, keep
it in one, by passing the store's name and a serde for its values:
`joinOnForeignKey(named, other, value, otherKey, otherValue, Store("results"), serde)`.
`Topology.of` refuses a program that needs this and does not do it. A step
after the result that keeps a store of its own, such as `mapValues` given a
`Store`, can be joined or grouped through that store instead. A `filter` of the
result runs without a store, as in Java, and makes a delete of every value it
drops, whether or not the key held a value that passed before.

With the name `order-customer`, the join adds these to the application:

| What | Name |
| --- | --- |
| A topic | `<application id>-order-customer-subscription-registration-topic` |
| A topic | `<application id>-order-customer-subscription-response-topic` |
| A store | `order-customer-subscription-store`, with its changelog |

Windowed tables cannot join on a foreign key, and there is no outer version. (Other
joins of windowed tables are in [Windows](windows.md#joining-windowed-tables).) If the
other table's topic is partitioned some other way than by Kafka's default, give that
table its partitioner (see [Choosing partitions](partitioning.md)).

If some members run Java Kafka Streams, see
[Mixing with Java Kafka Streams](java-kafka-streams.md#joins) for how to name the
join and build the Java program.

## Joining streams

Two streams with the same key join within windows of time. Each record joins every record of the other
stream with its key whose time is close enough to its own. Here orders and
payments are both keyed by the order, and each order should be paid within
five minutes:

```scala mdoc:compile-only
// Join orders with payments made within five minutes, and keep the unmatched ones too
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
      JoinWindows(5.minutes, grace = 30.seconds),  // At most five minutes apart, either way
      Store("payments"),                           // Names the stores that hold each stream's records
      order,
      item,
      cents
    ) {
      case Ior.Left(bought)       => s"$bought, unpaid"                // An order with no payment, written once its window and grace have passed
      case Ior.Right(paid)        => s"$paid cents paid for no order"  // A payment with no order, written once its window and grace have passed
      case Ior.Both(bought, paid) => s"$bought, paid $paid cents"      // An order and a payment at most five minutes apart
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
The stores are named from the store's name:

| Join        | Stores                                                                                                  |
|-------------|---------------------------------------------------------------------------------------------------------|
| `join`      | `<name>-this-join-store`, `<name>-other-join-store`                                                     |
| `leftJoin`  | `<name>-this-join-store`, `<name>-outer-other-join-store`, `<name>-left-shared-join-store`              |
| `outerJoin` | `<name>-outer-this-join-store`, `<name>-outer-other-join-store`, `<name>-outer-shared-join-store`       |

The three serdes write the key and each stream's values to the stores. A
stream whose key has changed is written first to a repartition topic named
after the join, `<application id>-<join>-left-repartition` or
`<application id>-<join>-right-repartition`, with the same serdes. A Java
program names everything the same way with `StreamJoined`, as
[Mixing with Java Kafka Streams](java-kafka-streams.md#writing-the-java-program)
shows.

A record without a key, one whose key serde writes none, is written alone at
once by a left join's left stream and by either stream of an outer join, and
is otherwise dropped.

The two streams are processed together, so their topics must have the same
number of partitions. To join a stream with itself, see
[Joining a stream or a table with itself](branching.md#joining-a-stream-or-a-table-with-itself).

## Joining a global table

A `GlobalKTable` holds the latest value of each key of a topic, and every member
of the application keeps all of it. It suits reference data small enough for each member to hold. A stream joins it by a
key taken from each record, so the two need not share a key or a number of
partitions. Here each order is joined with the product named in its value:

```scala mdoc:compile-only
// Join each order with the product named in its value
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersWithProducts(orders: Topic, products: Topic, output: Topic): Pipeline[IO] =
  val text    = Serde.utf8[IO]
  val catalog = GlobalKTable(Named("products"), products, text, text, Store("products"))  // Every member holds all of this table
  KStream(Named("orders"), orders, text, text)
    // Find the table's key in each record, then make the result
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

The application's `state` lists `StreamsProblem.JavaMembersMayBeRefused` for every
program with a global table, whether or not any members run Java Kafka Streams. An
application whose members all run xkafka can ignore it. If some members run Java
Kafka Streams, see [Mixing with Java Kafka Streams](java-kafka-streams.md#joins)
for how to build the Java program.

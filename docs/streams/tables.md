# Tables

A `KTable` holds the latest value of each key. Besides the tables that `count`,
`reduce` and `aggregate` make, a table can be read straight from a topic, as Java
Kafka Streams' `StreamsBuilder.table` reads it:

```scala mdoc:compile-only
// Read a table from a topic, and write every change to it
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def userNames(users: Topic, output: Topic): Pipeline[IO] =
  val user = Serde.utf8[IO]
  val name = Serde.utf8[IO]
  KTable(Named("users"), users, user, name, Store("users"))  // Keep each key's latest value in this store
    .toStream(Named("user-changes"))
    .to(Named("write-user-names"), output, user, name.option)  // A deleted key is None, which .option writes as a tombstone
```

Each record's value replaces its key's, even a record timestamped earlier than
the one it replaces. A record without a value (a tombstone) deletes its key, and
a record without a key is skipped. The table is kept in the named store, whose
changelog is `<application id>-<store>-changelog`, here
`<application id>-users-changelog`. If the topic is partitioned some other way than
by Kafka's default, give the table its partitioner (see
[Choosing partitions](README.md#choosing-partitions)).

A table read from a topic can delete keys, so its `toStream` gives `Option`
values: `Some` for an update and `None` for a delete, which `Serde.option`
writes as a tombstone. A table that only ever updates
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
// Keep each user's latest address as a table, and write every change to it
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def latestAddress(moves: Topic, output: Topic): Pipeline[IO] =
  val user    = Serde.utf8[IO]
  val address = Serde.utf8[IO]
  KStream(Named("moves"), moves, user, address.option)                    // A value of None will delete the user's address
    .toTable(Named("latest-address"), Store("addresses"), user, address)  // Keep each key's latest value in a store
    .toStream(Named("address-changes"))
    .to(Named("write-address-changes"), output, user, address.option)
```

## Filtering and changing a table

A table's `filter`, `filterNot` and `mapValues` give another table, which follows
every change to the first. `filter` and `filterNot` turn a value they drop into a
delete, so the result can always delete keys, and its changes are `Option`s. A
key that had no value in the result and still has none passes nothing on.
`mapValues` passes a delete on as a delete:

```scala mdoc:compile-only
// Join each order with the email of its user, unless their account is closed
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def activeEmails(users: Topic, orders: Topic, output: Topic): Pipeline[IO] =
  val id   = Serde.utf8[IO]
  val text = Serde.utf8[IO]
  val emails =
    KTable(Named("users"), users, id, text, Store("users"))
      .filterNot(Named("not-closed"))((_, user) => user.startsWith("closed:"))  // Leave closed accounts out of this table
      .mapValues(Named("email"))(_.split(' ').last)                             // Keep only the email from each value
  KStream(Named("orders"), orders, id, text)
    .join(Named("order-email"), emails)((order, email) => s"$order for $email")  // An order whose user has no email here is dropped
    .to(Named("write-order-emails"), output, id, text)
```

These tables keep no store of their own: a join reads through to the store behind
them, applying the filter and the change as it reads. To keep the result, which
adds a changelog topic, give a `Store` and a `Serde`, as in
`filter(Named("kept"), Store("kept"), text)(keep)`. `suppress` needs a serde for
the values it holds, so a table from `mapValues` must be kept in a store before it
is suppressed; `Topology.of` refuses it otherwise.

## Grouping a table

A table's `groupBy` puts its values in new groups, as Java Kafka Streams'
`KTable.groupBy` does, and `count`, `reduce` or `aggregate` keep a result for each
group in a store. A value can leave its group, when it is replaced or deleted, so
`reduce` and `aggregate` take two functions, one to put a value in and one to take
it out. Here each user's balance counts towards their country's total, and moving
to another country takes it out of one total and puts it in the other:

```scala mdoc:compile-only
// Keep a total per country, moving a user's balance when they change country
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def totals(balances: Topic, output: Topic): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  KTable(Named("balances"), balances, text, text, Store("balances"))
    .groupBy(Repartition("by-country"), text, amount): (_, balance) =>  // Choose each value's new group
      val Array(country, money) = balance.split(':')
      (country, money.toLong)
    .reduce(Named("total"), Store("totals"))(_ + _)(_ - _)  // One function puts a value in, the other takes it out
    .toStream(Named("total-changes"))
    .to(Named("write-totals"), output, text, amount.option)  // Each change is an Option, so write it with .option
```

Each change goes through the repartition topic
`<application id>-<name>-repartition`, which the application's Java Kafka Streams
members can read and write too. A group that loses its last value stays in the
result, so a count stays at 0. To leave such groups out, `filter` the result.

Changes to the result are `Option`s, which is why the example writes them with
`amount.option`. A change is `None` when a value leaves a group that holds nothing.
You can see this when you group a `count` by its value: a key's first count also
leaves the group for 0, so if that group is empty, its change is `None`.

## Folding streams together

`cogroup` folds several grouped streams into one store, as Java Kafka Streams'
`cogroup` does. Each group has its own function, and each can hold a different
type of value. Here orders and refunds, both keyed by the user, make one balance
for each user:

```scala mdoc:compile-only
// Fold orders and refunds into one balance per user
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def balances(orders: Topic, refunds: Topic, output: Topic): Pipeline[IO] =
  val user  = Serde.utf8[IO]
  val cents = Serde.long[IO]
  KStream(Named("orders"), orders, user, cents)
    .groupByKey(user, cents)
    .cogroup[Long]((_, paid, balance) => balance + paid)        // An order adds to the balance
    // A refund takes from the balance
    .cogroup(KStream(Named("refunds"), refunds, user, cents).groupByKey(user, cents))((_, refunded, balance) => balance - refunded)
    .aggregate(Named("balance"), Store("balances"), cents)(0L)  // Start from 0, and keep the result in one store
    .toStream(Named("balance-changes"))
    .to(Named("write-balances"), output, user, cents)
```

The result is a table like any other, and each change to it is written to the
store's changelog. Its steps take their names from the one you give:
`balance-cogroup-agg-0` and `balance-cogroup-agg-1` for the groups, and
`balance-cogroup-merge` for what comes out of them.

The groups' topics are processed together, so they must have the same number of
partitions. If they do not, the application's `state` lists
`StreamsProblem.IncorrectlyPartitionedTopics`. Groups that go through repartition
topics do not need this of the topics they read.

To fold two paths of one stream, group each with its `flow` and send the stream
into the result with `into`, as in [Branching](branching.md).

@:callout(warning)
Java Kafka Streams cannot start a program in which several groups are folded
together and any of them goes through a repartition topic. Such a program runs on
xkafka, but cannot have Java members, and the application's `state` lists
`StreamsProblem.JavaMembersMayBeRefused`, naming the topics.
@:@

Folding in windows works as for one group, with `windowedBy` before `aggregate`.
A session's `aggregate` also takes a function to merge two sessions:

```scala
// Fold the groups into one result per minute
grouped
  .windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))  // Window the groups before aggregating them
  .aggregate(Named("balance"), Store("balances"), cents)(0L)
```

A record is late by the latest time read from its own group, not from the others,
so a group that falls behind keeps its records, as in Java. As in Java, these
windows have no `emitOnWindowClose`. To send only each window's final result, use
[`suppress`](holding-back-updates.md).

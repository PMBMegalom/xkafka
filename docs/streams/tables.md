# Tables

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

## Filtering and changing a table

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

## Grouping a table

A table's `groupBy` puts its values in new groups, as Java Kafka Streams'
`KTable.groupBy` does, and `count`, `reduce` or `aggregate` keep a result for each
group in a store. A value can leave its group, when it is replaced or deleted, so
`reduce` and `aggregate` take two functions, one to put a value in and one to take
it out. Here each user's balance counts towards their country's total, and moving
to another country takes it out of one total and puts it in the other:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def totals(balances: Topic, output: Topic): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  KTable(Named("balances"), balances, text, text, Store("balances"))
    .groupBy(Repartition("by-country"), text, amount): (_, balance) =>
      val Array(country, money) = balance.split(':')
      (country, money.toLong)
    .reduce(Named("total"), Store("totals"))(_ + _)(_ - _)
    .toStream(Named("total-changes"))
    .to(Named("write-totals"), output, text, amount.option)
```

Each change goes through the repartition topic
`<application id>-<name>-repartition`, which the application's Java Kafka Streams
members can read and write too. A group that loses its last value stays in the
result, so a count stays at 0. To leave such groups out, `filter` the result.

Changes to the result are `Option`s, which is why the example writes them with
`amount.option`. A change is `None` when a value leaves a group that holds nothing.
You can see this when you group a `count` by its value: a key's first count also
leaves the group for 0, so if that group is empty, its change is `None`.

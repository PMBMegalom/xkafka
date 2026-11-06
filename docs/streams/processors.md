# Processors

A processor is a step you write yourself, for logic that the other steps cannot
express. It sees one record at a time with its time and headers, can read and
write its own stores, and returns the records to send on.

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def runningTotals(input: Topic, output: Topic): Pipeline[IO] =
  val user   = Serde.utf8[IO]
  val amount = Serde.long[IO]
  val totals = KeyValueStore.of(Store("totals"), user, amount)
  KStream(Named("payments"), input, user, amount)
    .process(
      Named("add-to-total"),
      Processor.withStore(totals): kept =>
        Processor.onRecord: record =>
          kept.get(record.key).flatMap: total =>
            val updated = total.getOrElse(0L) + record.value
            kept.put(record.key, updated).as(List(record.withValue(updated)))
    )
    .to(Named("write-totals"), output, user, amount)
```

`Processor.withStore` hands your code the store already typed, so you cannot
read a store you did not declare. Use `Processor.withStores` for two or three
stores, and `Processor.withoutStores` for none.

## What comes out

Each record you return goes on to the next step. A record made with `withKey` or
`withValue` keeps the time and headers of the one it was made from, and
`withTimestamp` and `withHeaders` change them. Return an empty list to send
nothing, or several records to send them all, in order.

The records you read also say where they came from: `record.origin` gives the
topic, partition and offset.

Since a processor can give a record any key, grouping its results needs a
repartition topic, so `groupByKey` asks you to name one.

## Stores

A `KeyValueStore` has `get`, `put`, `putIfAbsent`, `putAll` and `delete`, and
reads many keys with `all`, `range`, `prefixScan`, `reverseAll` and
`reverseRange`, which give an `fs2.Stream`.

Those reads follow the order of the keys' serialized bytes, which is not always
the order of the keys themselves. A number written as text, for example, sorts
`"10"` before `"9"`.

Your code reads what it has written straight away. The writes are kept when the
task commits, together with the records you returned and the offsets read, and
go to a changelog topic named `<application>-<store>-changelog`, which a member
taking over the work restores the store from. If your handler fails, nothing it
wrote for that record is kept.

Anything else your handler does, such as calling another service, runs again for
records replayed after a crash or a fencing.

## Sharing a store

Give two processors the same `KeyValueStore.of` value and they share one store.
They then run in one sub-topology, so a task handles the same partition of both
inputs. Two separate values with the same store name are refused when you call
`Topology.of`.

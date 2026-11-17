# Choosing partitions

By default, each record goes to the partition Kafka's producers choose for its key,
so all of a key's records are in one partition, in order. A `Partitioner` chooses
some other way. You can give one in three places:

| Where | What you can give |
| --- | --- |
| `to` | `partitioner`, which chooses each record's partition in the output topic |
| `repartition` | `partitions`, how many the repartition topic has, and `partitioner` |
| `KTable(...)`, read from a topic | `partitioner`, saying how that topic is partitioned |

```scala mdoc:compile-only
// Keep each region's orders together: through a repartition topic of twelve partitions, then to the output, both partitioned by region.
// An order's key starts with its region, as in "eu-1042".
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def ordersByRegion(orders: Topic, output: Topic): Pipeline[IO] =
  val text   = Serde.utf8[IO]
  val region = Partitioner.byKey((key: String, count: Int) => Math.floorMod(key.takeWhile(_ != '-').hashCode, count))  // The same region, the same partition
  KStream(Named("orders"), orders, text, text)
    .repartition(Repartition("orders-by-region"), text, text, partitions = Some(12), partitioner = Some(region))      // Twelve partitions, by region
    .to(Named("write-orders"), output, text, text, Some(region))                                                   // The output is partitioned by region too
```

Without `partitions`, the broker chooses how many partitions the repartition topic
has. A count under one is refused when you call `Topology.of`.

## Writing a partitioner

A partitioner is given a record's key, value and headers, and how many partitions
the topic has. It gives back a partition, from 0 up to one less than that number.
Any other number stops the application, with an error naming the topic.

```scala mdoc:compile-only
// Two partitioners: one that goes by the key alone, and one that also reads a header
import xkafka.*
import xkafka.streams.*

// Customers' ids are numbers after a letter, as in "c42": spread customers by that number, each customer always in the same partition
val byCustomer = Partitioner.byKey((key: String, count: Int) => Math.floorMod(key.drop(1).toIntOption.getOrElse(0), count))

// Records with the header priority=urgent go to partition 0, and the rest are spread by key over the other partitions
val urgentFirst = Partitioner[String, String]: (key, _, headers, count) =>
  val urgent = headers.get[String]("priority").contains(Right("urgent"))
  if urgent || count == 1 then 0 else 1 + Math.floorMod(key.hashCode, count - 1)
```

- Use `Partitioner.byKey` when the key alone decides, and `Partitioner(...)` when the
  value or headers do too.
- Only records in the same partition stay in order, so a partitioner that does not
  go by the key alone can put one key's records out of order.
- Give the same answer for the same record every time. Records processed again, such
  as after a crash, should land where they did before.
- `hashCode` can be negative: `Math.floorMod`, unlike `%`, always gives a number
  from 0 up.

## Java partitioners (JVM only)

On the JVM, a `StreamPartitioner` written for Java Kafka Streams can be used as a
partitioner. It comes with the `xkafka-streams-java` module (see
[Adding the module](java-kafka-streams.md#adding-the-module)):

```scala mdoc:compile-only
// Turn a StreamPartitioner your Java Kafka Streams application already uses into a partitioner for xkafka
import org.apache.kafka.streams.processor.StreamPartitioner

import xkafka.streams.*
import xkafka.streams.interop.*                                       // Adds Partitioner.fromJava, on the JVM only

def sameAsJava(java: StreamPartitioner[String, String]): Partitioner[String, String] = Partitioner.fromJava(java)
```

It is given each record's topic, key, value, headers and partition count. If it
chooses no partition, the record goes where Kafka's default puts it. If it chooses
several partitions, or an empty set, the application stops with an error naming
the topic, since xkafka sends each record to one partition.

## Tables whose topic is partitioned its own way

A table's topic may be written by another application that partitions it its own
way. Give the table that partitioner, made with `Partitioner.byKey`, since a table
needs each key to stay in one partition whatever its value. xkafka then uses it
whenever it sends records to be looked up in that table:

- a stream joined with the table after the stream's key changed;
- a [foreign-key join](joins.md#joining-on-a-foreign-key) looking up the table's rows.

Without it, those records go to partitions that do not hold their keys, and find
nothing.

If some members run Java Kafka Streams, see
[Partitioners](java-kafka-streams.md#partitioners).
